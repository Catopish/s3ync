use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart};
use tokio::io::AsyncReadExt;

use crate::config::Global;

/// Object metadata key holding the BLAKE3 hex hash of the uploaded content.
const HASH_META: &str = "s3ync-hash";

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
        hash: &str,
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
            .metadata(HASH_META, hash)
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
        hash: &str,
    ) -> Result<()> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .metadata(HASH_META, hash)
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
