package reconciliation


case class ReconcileMetadataColumn(
    clazz: ReconciledMetadataRecord,
    displaySize: ReconciledMetadataRecord,
    precision: ReconciledMetadataRecord,
    scale: ReconciledMetadataRecord,
    currency: ReconciledMetadataRecord,
    nullable: ReconciledMetadataRecord
)

case class ReconciledMetadataRecord (
	source: Any,
	target: Any,
	convertibleLevel: Convertible.LEVEL
)