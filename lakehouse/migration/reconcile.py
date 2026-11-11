"""Generic read-only Parquet/Iceberg reconciliation driven by TableSpec."""
import argparse, json, os, sys
from minio import Minio
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from common.bounded_e2e_scope import build_bounded_e2e_scope, TEST_BUCKET
from lakehouse.evidence import publish_pass_evidence_for_attestation
from lakehouse.attestation import read_snapshot_attestation
from lakehouse.migration.manifest import _identity, read_manifest
from lakehouse.migration.manifest import discover_manifests
from lakehouse.models.spec import SPECS


def scope_target_to_source_keys(target, source, key):
    """Limit a full Iceberg table to keys represented by the source scope."""
    return target.join(source.select(key).distinct(), key, "left_semi")


def read_iceberg_snapshot(spark, identifier, snapshot_id=None):
    """Read the exact Iceberg snapshot authorized by migration evidence."""
    if snapshot_id is None:
        return spark.table(identifier)
    return (spark.read.format("iceberg")
            .option("snapshot-id", str(int(snapshot_id)))
            .load(identifier))

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--table-name", choices=tuple(SPECS), required=True); parser.add_argument("--attestation-identity"); parser.add_argument("--manifest-key"); args=parser.parse_args(); spec=SPECS[args.table_name]
    settings=get_minio_settings(); bucket=os.environ["BRONZE_BUCKET"]; scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None; client=Minio(settings["endpoint"],access_key=settings["access_key"],secret_key=settings["secret_key"],secure=False)
    silver_namespace = scope.silver_namespace if scope is not None else "silver"
    # A manifest-scoped reconciliation must compare this batch's logical key
    # scope with the corresponding rows in the full Iceberg table. Comparing a
    # 27-row current batch with a 6,316-row historical table head is invalid.
    manifests = ([(args.manifest_key, read_manifest(client, bucket, args.manifest_key, spec.name, scope))]
                 if args.manifest_key else discover_manifests(client,bucket,spec.name,scope))
    if not manifests: raise ValueError(f"No completed {spec.name} manifests found")
    builder = SparkSession.builder.appName(f"ReconcileIceberg{spec.name.title().replace('_','')}")
    if scope is not None:
        builder = (builder
                   .config("spark.sql.catalog.lakehouse.warehouse", scope.warehouse)
                   .config("spark.sql.catalog.lakehouse.ref", scope.nessie_ref))
    spark=builder.getOrCreate()
    try:
        configure_s3a(spark); frames=[spark.read.parquet(f"s3a://{bucket}/{m['data_prefix']}") for _,m in manifests]; legacy=frames[0]
        for frame in frames[1:]: legacy=legacy.unionByName(frame)
        hash_expr=F.sha2(F.concat_ws("||",*[F.coalesce(F.col(c).cast("string"),F.lit("")) for c in spec.columns]),256)
        if spec.reconcile_watermark: legacy=legacy.withColumn("_hash",hash_expr).withColumn("_rank",F.row_number().over(Window.partitionBy(spec.key).orderBy(F.col(spec.reconcile_watermark).desc(),F.col("_hash").desc()))).filter("_rank=1").drop("_hash","_rank")
        snapshot_id = None
        manifest = None
        record = None
        if args.manifest_key:
            manifest = read_manifest(client, bucket, args.manifest_key, spec.name, scope)
            progress = f"lakehouse.metadata.{spec.name}_parquet_migration_progress"
            record = spark.sql(
                f"SELECT snapshot_id FROM {progress} "
                f"WHERE manifest_identity = '{_identity(manifest)}'"
            ).first()
            if record is None:
                raise RuntimeError("Missing migration progress for reconciliation evidence")
            snapshot_id = int(record.snapshot_id)
        iceberg = read_iceberg_snapshot(spark, f"lakehouse.{silver_namespace}.{spec.name}", snapshot_id)
        # MERGE preserves historical rows. Restrict the target to keys present
        # in the source manifest before parity/hash comparison.
        iceberg_scope = scope_target_to_source_keys(iceberg, legacy, spec.key)
        hashed=lambda frame:frame.select(spec.key,hash_expr.alias("business_hash")); diff=hashed(legacy).exceptAll(hashed(iceberg_scope)).unionByName(hashed(iceberg_scope).exceptAll(hashed(legacy)))
        metrics=lambda frame:{"rows":frame.count(),"distinct_keys":frame.select(spec.key).distinct().count(),**({"total":str(frame.agg(F.sum(spec.aggregate).alias("n")).first().n or 0),"status_totals":{r[0]:str(r[1]) for r in frame.groupBy(spec.status).agg(F.sum(spec.aggregate)).collect()}} if spec.aggregate else {})}
        fks={column:iceberg_scope.select(column).distinct().join(spark.table(f"lakehouse.{silver_namespace}.{parent}").select(column).distinct(),column,"left_anti").count() for column,parent in (spec.foreign_keys or {}).items()}; result={"legacy":metrics(legacy),"iceberg":metrics(iceberg_scope),"business_hash_mismatches":diff.count(),"missing_fks":fks,"manifest_count":len(manifests),"scope":"manifest_keys" if args.manifest_key else "all_manifests"}
        if result["legacy"]!=result["iceberg"] or result["business_hash_mismatches"] or any(fks.values()): raise RuntimeError(f"{spec.name} reconciliation failed: {result}")
        attestation_identity = args.attestation_identity
        if args.manifest_key:
            manifest = manifest or read_manifest(client, bucket, args.manifest_key, spec.name, scope)
            progress = f"lakehouse.metadata.{spec.name}_parquet_migration_progress"
            record = record or spark.sql(f"SELECT snapshot_id FROM {progress} WHERE manifest_identity = '{_identity(manifest)}'").first()
            if record is None: raise RuntimeError("Missing migration progress for reconciliation evidence")
            candidate = spark.sql(f"SELECT snapshot_id FROM lakehouse.{silver_namespace}.{spec.name}.snapshots WHERE snapshot_id = {int(record.snapshot_id)}").first()
            if candidate is None: raise RuntimeError("Manifest snapshot is missing from Iceberg metadata")
            # Attestation task must have published this exact immutable proof.
            from lakehouse.attestation import build_snapshot_attestation
            rows = spark.sql(f"SELECT manifest_identity FROM {progress} WHERE snapshot_id = {int(record.snapshot_id)}").collect()
            attestation_identity = build_snapshot_attestation(table=spec.name, snapshot_id=int(record.snapshot_id), manifest_identities=[row.manifest_identity for row in rows], catalog="lakehouse", namespace=silver_namespace, ref=scope.nessie_ref if scope is not None else "main")["identity"]
        if attestation_identity:
            snapshot = (spark.sql(f"SELECT snapshot_id FROM lakehouse.{silver_namespace}.{spec.name}.snapshots WHERE snapshot_id = {snapshot_id}").first()
                        if snapshot_id is not None else spark.sql(f"SELECT snapshot_id FROM lakehouse.{silver_namespace}.{spec.name}.snapshots ORDER BY committed_at DESC LIMIT 1").first())
            if snapshot is None: raise RuntimeError("Iceberg snapshot is missing")
            evidence = publish_pass_evidence_for_attestation(
                client, bucket, kind="reconciliation", table=spec.name,
                snapshot_id=int(snapshot.snapshot_id), details=result,
                attestation_identity=attestation_identity,
                namespace=silver_namespace,
                ref=scope.nessie_ref if scope is not None else "main",
                scope=scope,
            )
            result["reconciliation_evidence_identity"] = evidence["identity"]
        snapshot = (spark.sql(f"SELECT snapshot_id FROM lakehouse.{silver_namespace}.{spec.name}.snapshots WHERE snapshot_id = {snapshot_id}").first()
                    if snapshot_id is not None else spark.sql(f"SELECT snapshot_id FROM lakehouse.{silver_namespace}.{spec.name}.snapshots ORDER BY committed_at DESC LIMIT 1").first())
        if snapshot is None or not result.get("reconciliation_evidence_identity"):
            raise RuntimeError("PASS reconciliation requires attested snapshot evidence")
        result.update({"status": "PASS", "table_name": spec.name, "snapshot_id": int(snapshot.snapshot_id)})
        print(json.dumps(result,sort_keys=True))
    finally: spark.stop()
if __name__=="__main__": main()
