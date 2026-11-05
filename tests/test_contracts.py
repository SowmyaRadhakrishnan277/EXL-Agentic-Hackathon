from factory.contracts.models import (
    LegacyJob, DiscoveryResult, ColumnLineage, DesignSpec, DataContract,
    ColumnContract, Layer, SourceLanguage,
)


def test_contract_round_trip():
    job = LegacyJob(job_id="J01", language=SourceLanguage.SAS, path="legacy/sas/J01.sas",
                    code="data x; set claims; run;")
    disc = DiscoveryResult(
        job_id=job.job_id, input_tables=["claims"], output_tables=["x"],
        column_lineage=[ColumnLineage(target_table="x", target_column="paid_amount",
                                      source_columns=["claims.paid_amount"], transformation="copy")],
        summary="copies claims",
    )
    spec = DesignSpec(job_id="J01", layer=Layer.BRONZE, target_tables=["bronze.x"],
                      contracts=[DataContract(table="bronze.x", primary_key=["claim_id"],
                                              columns=[ColumnContract(name="claim_id", dtype="string", nullable=False)])])
    for m in (job, disc, spec):
        assert type(m).model_validate_json(m.model_dump_json()) == m
