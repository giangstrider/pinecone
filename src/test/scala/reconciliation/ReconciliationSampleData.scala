package reconciliation

import reconciliation.QueryStage.{PrepareQuery, ReconciliationRecord}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}

import java.util.Calendar

object ReconciliationSampleData {
	val sampleAuditDate = {
		val cal = Calendar.getInstance
		cal.set(Calendar.YEAR, 2021);
		cal.set(Calendar.MONTH, Calendar.JUNE);
		cal.set(Calendar.DAY_OF_MONTH, 15);
		cal.getTime
	}
	val sampleDummyMetadata = Some(QueryMetadataColumn(30, 20, 0, false, false))
	val dummyAuditDateReconciledMetadata = Some(ReconcileMetadataColumn(
		ReconciledMetadataRecord(sampleAuditDate.getClass, sampleAuditDate.getClass, Convertible.Good),
		ReconciledMetadataRecord(30, 30, Convertible.Good),
		ReconciledMetadataRecord(20, 20, Convertible.Good),
		ReconciledMetadataRecord(0, 0, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good)
	))
	val dummyStringDateReconciledMetadata = Some(ReconcileMetadataColumn(
		ReconciledMetadataRecord("Pinecone".getClass, "Pinecone".getClass, Convertible.Good),
		ReconciledMetadataRecord(30, 30, Convertible.Good),
		ReconciledMetadataRecord(20, 20, Convertible.Good),
		ReconciledMetadataRecord(0, 0, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good)
	))
	val dummyIntegerDateReconciledMetadata = Some(ReconcileMetadataColumn(
		ReconciledMetadataRecord(0.asInstanceOf[Integer].getClass, 0.asInstanceOf[Integer].getClass, Convertible.Good),
		ReconciledMetadataRecord(30, 30, Convertible.Good),
		ReconciledMetadataRecord(20, 20, Convertible.Good),
		ReconciledMetadataRecord(0, 0, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good)
	))
	val dummyDoubleDateReconciledMetadata = Some(ReconcileMetadataColumn(
		ReconciledMetadataRecord((1.0: java.lang.Double).getClass, (1.0: java.lang.Double).getClass, Convertible.Good),
		ReconciledMetadataRecord(30, 30, Convertible.Good),
		ReconciledMetadataRecord(20, 20, Convertible.Good),
		ReconciledMetadataRecord(0, 0, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good),
		ReconciledMetadataRecord(false, false, Convertible.Good)
	))

	object GeneralSingleQueryKeyAndReconcileKey {
		val sourceGreaterThanTarget = {
			val queryKey = "transaction_by_department"
			val prepareQuery = PrepareQuery(
				queryKey, "sales_db",
				"SELECT department, transaction_date, store_number, audit_date, count(*) as count, sum(amount) as sales FROM sales GROUP BY department,transaction_date",
				"sales_data_mart",
				"SELECT department, transaction_date, store_number, audit_date, count(*) as count, sum(amount) as sales FROM mart GROUP BY department,transaction_date",
				1, List("department", "transaction_date")
			)

			val sourceRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("store_name", Some("Pinecone"), sampleDummyMetadata),
						QueryColumn("audit_date", Some(sampleAuditDate), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("store_name", Some("Pinecone"), sampleDummyMetadata),
						QueryColumn("audit_date", Some(sampleAuditDate), sampleDummyMetadata),
						QueryColumn("count", Some(47532), sampleDummyMetadata),
						QueryColumn("sales", Some(37680362.15), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("store_name", Some("Pinecone"), sampleDummyMetadata),
						QueryColumn("audit_date", Some(sampleAuditDate), sampleDummyMetadata),
						QueryColumn("count", Some(10065), sampleDummyMetadata),
						QueryColumn("sales", Some(15630320.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("store_name", Some("Pinecone"), sampleDummyMetadata),
						QueryColumn("audit_date", Some(sampleAuditDate), sampleDummyMetadata),
						QueryColumn("count", Some(47531), sampleDummyMetadata),
						QueryColumn("sales", Some(37680322.15), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("audit_date", false, StringLikeColumn(Some(sampleAuditDate), Some(sampleAuditDate), isMatched = true), dummyAuditDateReconciledMetadata),
						ReconciledColumn("count", false, NumberColumn[Double](Some(47532.0), Some(47531.0), 1.0, 0.0021038458, true, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Fashion"), Some("Fashion"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales", false, NumberColumn[Double](Some(37680362.15), Some(37680322.15), 40.0, 0.0001061561, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("store_name", false, StringLikeColumn(Some("Pinecone"), Some("Pinecone"), isMatched = true), dummyStringDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata),
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("audit_date" ,false, StringLikeColumn(Some(sampleAuditDate), Some(sampleAuditDate), isMatched = true), dummyAuditDateReconciledMetadata),
						ReconciledColumn("count" ,false, NumberColumn[Double](Some(10821.0), Some(10065.0), 756, 6.9864153036, false, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), Some("Technology"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales" ,false, NumberColumn[Double](Some(15630330.46), Some(15630320.46), 10.0, 0.0000639782, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("store_name", false, StringLikeColumn(Some("Pinecone"), Some("Pinecone"), isMatched = true), dummyStringDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata))
					)
				)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}

		val targetGreaterThanSource = {
			val queryKey = "transaction_by_department"
			val prepareQuery = PrepareQuery(
				queryKey, "sales_db",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM sales GROUP BY department,transaction_date",
				"sales_data_mart",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM mart GROUP BY department,transaction_date",
				1, List("department", "transaction_date")
			)

			val sourceRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10065), sampleDummyMetadata),
						QueryColumn("sales", Some(15630320.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(47531), sampleDummyMetadata),
						QueryColumn("sales", Some(37680322.15), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(47532), sampleDummyMetadata),
						QueryColumn("sales", Some(37680362.15), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("count" ,false, NumberColumn[Double](Some(47531.0), Some(47532.0), -1.0, -0.0021038901, true, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Fashion"), Some("Fashion"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales" ,false, NumberColumn[Double](Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("count" ,false, NumberColumn[Double](Some(10065.0), Some(10821.0), -756, -7.5111773472, false, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), Some("Technology"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales" ,false, NumberColumn[Double](Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata)
					)
				)
			)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}

		val targetMissingReconcileKeyOfSource = {
			val queryKey = "transaction_by_department"
			val prepareQuery = PrepareQuery(
				queryKey, "sales_db",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM sales GROUP BY department,transaction_date",
				"sales_data_mart",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM mart GROUP BY department,transaction_date",
				1, List("department", "transaction_date")
			)

			val sourceRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-11"), sampleDummyMetadata),
						QueryColumn("count", Some(10409), sampleDummyMetadata),
						QueryColumn("sales", Some(14630732.52), sampleDummyMetadata)
					),
					"Technology|2021-06-11"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("count", false, NumberColumn[Double](Some(10821.0), Some(10821.0), 0.0, 0.0, true, isMatched = true), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), Some("Technology"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales" ,false, NumberColumn[Double](Some(15630330.46), Some(15630330.46), 0.0, 0.0, true, isMatched = true), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), None, false), None),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-11"), None, false), None),
						ReconciledColumn("count", false, NumberColumn[Double](Some(10409.0), None, 10409.0, 100.0, false, isMatched = false), None),
						ReconciledColumn("sales" ,false, NumberColumn[Double](Some(14630732.52), None, 14630732.52, 100.0, false, isMatched = false), None)
					)
				)
			)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}

		val bothMissingReconcileKeyOfEachOther = {
			val queryKey = "transaction_by_department"
			val prepareQuery = PrepareQuery(
				queryKey, "sales_db",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM sales GROUP BY department,transaction_date",
				"sales_data_mart",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM mart GROUP BY department,transaction_date",
				1, List("department", "transaction_date")
			)

			val sourceRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-11"), sampleDummyMetadata),
						QueryColumn("count", Some(10409), sampleDummyMetadata),
						QueryColumn("sales", Some(14630732.52), sampleDummyMetadata)
					),
					"Technology|2021-06-11"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(

						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), None, false), None),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), None, false), None),
						ReconciledColumn("count", false, NumberColumn[Double](Some(10821.0), None, 10821.0, 100.0, false, isMatched = false), None),
						ReconciledColumn("sales" ,false, NumberColumn[Double](Some(15630330.46), None, 15630330.46, 100.0, false, isMatched = false), None)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), None, false), None),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-11"), None, false), None),
						ReconciledColumn("count", false, NumberColumn[Double](Some(10409.0), None, 10409.0, 100.0, false, isMatched = false), None),
						ReconciledColumn("sales", false, NumberColumn[Double](Some(14630732.52), None, 14630732.52, 100.0, false, isMatched = false), None)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("department", true, StringLikeColumn(None, Some("Fashion"), false), None),
						ReconciledColumn("transaction_date", true, StringLikeColumn(None, Some("2021-06-10"), false), None),
						ReconciledColumn("count", false, NumberColumn[Double](None, Some(10821.0), -10821.0, -100.0, false, isMatched = false), None),
						ReconciledColumn("sales", false, NumberColumn[Double](None, Some(15630330.46), -15630330.46, -100.0, false, isMatched = false), None)
					)
				)
			)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}
	}

	object ColumnDifferent {
		val sourceMissAttributesCompareToTarget = {
			val queryKey = "transaction_by_department"
			val prepareQuery = PrepareQuery(
				queryKey, "sales_db",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM sales GROUP BY department,transaction_date",
				"sales_data_mart",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM mart GROUP BY department,transaction_date",
				1, List("department", "transaction_date")
			)

			val sourceRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(10065), sampleDummyMetadata),
						QueryColumn("sales", Some(15630320.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("count", Some(47531), sampleDummyMetadata),
						QueryColumn("sales", Some(37680322.15), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("audit_date", Some(sampleAuditDate), sampleDummyMetadata),
						QueryColumn("count", Some(10821), sampleDummyMetadata),
						QueryColumn("sales", Some(15630330.46), sampleDummyMetadata)
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), sampleDummyMetadata),
						QueryColumn("transaction_date", Some("2021-06-10"), sampleDummyMetadata),
						QueryColumn("audit_date", Some(sampleAuditDate), sampleDummyMetadata),
						QueryColumn("count", Some(47532), sampleDummyMetadata),
						QueryColumn("sales", Some(37680362.15), sampleDummyMetadata)
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("count", false, NumberColumn[Double](Some(47531.0), Some(47532.0), -1.0, -0.0021038901, true, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Fashion"), Some("Fashion"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales", false, NumberColumn[Double](Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("audit_date" ,false, StringLikeColumn(None, Some(sampleAuditDate), isMatched = false), None),
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("count", false, NumberColumn[Double](Some(10065.0), Some(10821.0), -756, -7.5111773472, false, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), Some("Technology"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales", false, NumberColumn[Double](Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("audit_date" ,false, StringLikeColumn(None, Some(sampleAuditDate), isMatched = false), None)
					)
				)
			)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}

		val targetMissAttributesCompareToSource = {
			val queryKey = "transaction_by_department"
			val prepareQuery = PrepareQuery(
				queryKey, "sales_db",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM sales GROUP BY department,transaction_date",
				"sales_data_mart",
				"SELECT department, transaction_date, count(*) as count, sum(amount) as sales FROM mart GROUP BY department,transaction_date",
				1, List("department", "transaction_date")
			)

			val sourceRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("transaction_date", Some("2021-06-10"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("audit_date", Some(sampleAuditDate), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("count", Some(10065), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("sales", Some(15630320.46), Some(QueryMetadataColumn(30, 20, 0, false, false)))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("transaction_date", Some("2021-06-10"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("audit_date", Some(sampleAuditDate), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("count", Some(47531), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("sales", Some(37680322.15), Some(QueryMetadataColumn(30, 20, 0, false, false)))
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("transaction_date", Some("2021-06-10"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("count", Some(10821), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("sales", Some(15630330.46), Some(QueryMetadataColumn(30, 20, 0, false, false)))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("transaction_date", Some("2021-06-10"), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("count", Some(47532), Some(QueryMetadataColumn(30, 20, 0, false, false))),
						QueryColumn("sales", Some(37680362.15), Some(QueryMetadataColumn(30, 20, 0, false, false)))
					),
					"Fashion|2021-06-10"
				)
			)


			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("audit_date" ,false, StringLikeColumn(Some(sampleAuditDate), None, isMatched = false), None),
						ReconciledColumn("count", false, NumberColumn[Double](Some(47531.0), Some(47532.0), -1.0, -0.0021038901, true, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Fashion"), Some("Fashion"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales", false, NumberColumn[Double](Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconciledColumn("audit_date" ,false, StringLikeColumn(Some(sampleAuditDate), None, isMatched = false), None),
						ReconciledColumn("count", false, NumberColumn[Double](Some(10065.0), Some(10821.0), -756, -7.5111773472, false, isMatched = false), dummyIntegerDateReconciledMetadata),
						ReconciledColumn("department", true, StringLikeColumn(Some("Technology"), Some("Technology"), true), dummyStringDateReconciledMetadata),
						ReconciledColumn("sales", false, NumberColumn[Double](Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782, true, isMatched = false), dummyDoubleDateReconciledMetadata),
						ReconciledColumn("transaction_date", true, StringLikeColumn(Some("2021-06-10"), Some("2021-06-10"), true), dummyStringDateReconciledMetadata)
					)
				)
			)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}
	}

	object EmptyResult {}
	object TypedAndMetadataValueInDetail {}
	object TypeMismatchForPairColumn {}
}