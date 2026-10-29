use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Global;

/// Object metadata key holding the BLAKE3 hex hash of the uploaded content.
/// Not set on encrypted objects: a plaintext hash would let anyone with bucket access
/// confirm guesses about the content.
const HASH_META: &str = "s3ync-hash";

pub struct Version {
    pub id: String,
    pub modified_unix: i64,
    pub size: u64,
    pub latest: bool,
}

#[derive(Clone)]
pub struct S3 {
    client: Client,
    multipart_threshold: u64,
    part_size: usize,
}

impl S3 {
    /// Credentials, region and endpoint come from the standard AWS env vars / config files.
    pub async fn new(global: &Global) -> Result<Self> {
        let sdk = aws_config::defaults(BehaviorVersion::latest()).load().await;
        if sdk.region().is_none() {
            bail!("no AWS region configured (set AWS_REGION)");
        }
        let conf = aws_sdk_s3::config::Builder::from(&sdk)
            .force_path_style(global.force_path_style)
            .build();
        Ok(Self {
            client: Client::from_conf(conf),
            multipart_threshold: global.multipart_threshold_mb * 1024 * 1024,
            part_size: (global.part_size_mb * 1024 * 1024) as usize,
        })
    }

    /// Checks the bucket exists and we can list under `prefix`.
    pub async fn check(&self, bucket: &str, prefix: &str) -> Result<()> {
        if let Err(e) = self.client.head_bucket().bucket(bucket).send().await {
            let e = e.into_service_error();
            if e.is_not_found() {
                bail!("bucket {bucket:?} does not exist (s3ync does not create buckets)");
            }
            return Err(anyhow::anyhow!(e)).with_context(|| {
                format!("bucket {bucket:?} not reachable (credentials/region ok?)")
            });
        }
        self.client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(1)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(e.into_service_error()))
            .with_context(|| format!("cannot list s3://{bucket}/{prefix}"))?;
        Ok(())
    }

    /// Whether bucket versioning is enabled. Errors if we may not ask (s3:GetBucketVersioning).
    pub async fn versioning_enabled(&self, bucket: &str) -> Result<bool> {
        let out = self
            .client
            .get_bucket_versioning()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(e.into_service_error()))
            .with_context(|| format!("get versioning of bucket {bucket:?}"))?;
        Ok(out.status() == Some(&BucketVersioningStatus::Enabled))
    }

    /// key -> size for every object under `prefix`.
    pub async fn list(&self, bucket: &str, prefix: &str) -> Result<HashMap<String, u64>> {
        let mut out = HashMap::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page
                .map_err(|e| anyhow::anyhow!(e.into_service_error()))
                .with_context(|| format!("listing s3://{bucket}/{prefix}"))?;
            for obj in page.contents() {
                if let Some(k) = obj.key() {
                    out.insert(k.to_string(), obj.size().unwrap_or(0).max(0) as u64);
                }
            }
        }
        Ok(out)
    }

    /// The s3ync hash stored on an object, `None` if the object or the metadata is missing.
    pub async fn remote_hash(&self, bucket: &str, key: &str) -> Result<Option<String>> {
        match self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => Ok(out.metadata().and_then(|m| m.get(HASH_META)).cloned()),
            Err(e) => {
                let e = e.into_service_error();
                if e.is_not_found() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!(e)).with_context(|| format!("head s3://{bucket}/{key}"))
                }
            }
        }
    }

    pub async fn upload(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        size: u64,
        hash: Option<&str>,
    ) -> Result<()> {
        if size >= self.multipart_threshold {
            return self.upload_multipart(bucket, key, path, hash).await;
        }
        let body = ByteStream::from_path(path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .set_metadata(hash.map(|h| [(HASH_META.to_string(), h.to_string())].into()))
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(e.into_service_error()))
            .with_context(|| format!("put s3://{bucket}/{key}"))?;
        Ok(())
    }

    async fn upload_multipart(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        hash: Option<&str>,
    ) -> Result<()> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .set_metadata(hash.map(|h| [(HASH_META.to_string(), h.to_string())].into()))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(e.into_service_error()))
            .with_context(|| format!("create multipart s3://{bucket}/{key}"))?;
        let upload_id = created
            .upload_id()
            .context("no upload id returned")?
            .to_string();

        match self.upload_parts(bucket, key, path, &upload_id).await {
            Ok(parts) => {
                self.client
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!(e.into_service_error()))
                    .with_context(|| format!("complete multipart s3://{bucket}/{key}"))?;
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(e)
            }
        }
    }

    async fn upload_parts(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        upload_id: &str,
    ) -> Result<Vec<CompletedPart>> {
        let mut file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("opening {}", path.display()))?;
        let mut parts = Vec::new();
        let mut part_number = 1;
        loop {
            let mut buf = Vec::with_capacity(self.part_size);
            (&mut file)
                .take(self.part_size as u64)
                .read_to_end(&mut buf)
                .await?;
            // An empty first part is still needed for a (shrunk to) empty file.
            if buf.is_empty() && part_number > 1 {
                break;
            }
            let last = buf.len() < self.part_size;
            let out = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(buf))
                .send()
                .await
                .map_err(|e| anyhow::anyhow!(e.into_service_error()))
                .with_context(|| format!("upload part {part_number} of s3://{bucket}/{key}"))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .set_e_tag(out.e_tag().map(str::to_string))
                    .build(),
            );
            if last {
                break;
            }
            part_number += 1;
        }
        Ok(parts)
    }

    pub async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let e = e.into_service_error();
                if e.is_not_found() {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!(e)).with_context(|| format!("head s3://{bucket}/{key}"))
                }
            }
        }
    }

    /// Streams an object (or one version of it) into `dst`; returns its s3ync hash, if any.
    pub async fn download(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        dst: &mut tokio::fs::File,
    ) -> Result<Option<String>> {
        let out = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version.map(str::to_string))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(e.into_service_error()))
            .with_context(|| format!("get s3://{bucket}/{key}"))?;
        let hash = out.metadata().and_then(|m| m.get(HASH_META)).cloned();
        let mut body = out.body.into_async_read();
        tokio::io::copy(&mut body, dst)
            .await
            .with_context(|| format!("downloading s3://{bucket}/{key}"))?;
        dst.flush().await?;
        Ok(hash)
    }

    /// Versions of exactly `key`, newest first. Needs s3:ListBucketVersions.
    pub async fn versions(&self, bucket: &str, key: &str) -> Result<Vec<Version>> {
        let mut out = Vec::new();
        let (mut key_marker, mut version_marker) = (None, None);
        loop {
            let page = self
                .client
                .list_object_versions()
                .bucket(bucket)
                .prefix(key)
                .set_key_marker(key_marker.take())
                .set_version_id_marker(version_marker.take())
                .send()
                .await
                .map_err(|e| anyhow::anyhow!(e.into_service_error()))
                .with_context(|| format!("listing versions of s3://{bucket}/{key}"))?;
            for v in page.versions() {
                if v.key() != Some(key) {
                    continue;
                }
                out.push(Version {
                    id: v.version_id().unwrap_or("null").to_string(),
                    modified_unix: v.last_modified().map(|t| t.secs()).unwrap_or(0),
                    size: v.size().unwrap_or(0).max(0) as u64,
                    latest: v.is_latest().unwrap_or(false),
                });
            }
            if page.is_truncated() != Some(true) {
                break;
            }
            key_marker = page.next_key_marker().map(str::to_string);
            version_marker = page.next_version_id_marker().map(str::to_string);
        }
        out.sort_by_key(|v| std::cmp::Reverse(v.modified_unix));
        Ok(out)
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(e.into_service_error()))
            .with_context(|| format!("delete s3://{bucket}/{key}"))?;
        Ok(())
    }
}
