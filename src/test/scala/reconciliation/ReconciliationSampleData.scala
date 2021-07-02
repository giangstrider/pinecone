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
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("store_name", Some("Pinecone"), "java.lang.String"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("store_name", Some("Pinecone"), "java.lang.String"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(47532), "java.lang.Integer"),
						QueryColumn("sales", Some(37680362.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("store_name", Some("Pinecone"), "java.lang.String"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(10065), "java.lang.Integer"),
						QueryColumn("sales", Some(15630320.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("store_name", Some("Pinecone"), "java.lang.String"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(47531), "java.lang.Integer"),
						QueryColumn("sales", Some(37680322.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						StringLikeColumn("audit_date", Some(sampleAuditDate), Some(sampleAuditDate), isMatched = true),
						NumberColumn[Double]("count", Some(47532.0), Some(47531.0), 1.0, 0.0021038458, isMatched = false),
						NumberColumn[Double]("sales", Some(37680362.15), Some(37680322.15), 40.0, 0.0001061561, isMatched = false),
						StringLikeColumn("store_name", Some("Pinecone"), Some("Pinecone"), isMatched = true)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						StringLikeColumn("audit_date", Some(sampleAuditDate), Some(sampleAuditDate), isMatched = true),
						NumberColumn[Double]("count", Some(10821.0), Some(10065.0), 756, 6.9864153036, isMatched = false),
						NumberColumn[Double]("sales", Some(15630330.46), Some(15630320.46), 10.0, 0.0000639782, isMatched = false),
						StringLikeColumn("store_name", Some("Pinecone"), Some("Pinecone"), isMatched = true)
					)
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
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10065), "java.lang.Integer"),
						QueryColumn("sales", Some(15630320.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(47531), "java.lang.Integer"),
						QueryColumn("sales", Some(37680322.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(47532), "java.lang.Integer"),
						QueryColumn("sales", Some(37680362.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", Some(47531.0), Some(47532.0), -1.0, -0.0021038901, isMatched = false),
						NumberColumn[Double]("sales", Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", Some(10065.0), Some(10821.0), -756, -7.5111773472, isMatched = false),
						NumberColumn[Double]("sales", Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782, isMatched = false)
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
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-11"), "java.sql.Date"),
						QueryColumn("count", Some(10409), "java.lang.Integer"),
						QueryColumn("sales", Some(14630732.52), "java.lang.Double")
					),
					"Technology|2021-06-11"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", Some(10821.0), Some(10821.0), 0.0, 0.0, isMatched = true),
						NumberColumn[Double]("sales", Some(15630330.46), Some(15630330.46), 0.0, 0.0, isMatched = true)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-11"),
					),
					List(
						NumberColumn[Double]("count", Some(10409.0), None, 10409.0, 100.0, isMatched = false),
						NumberColumn[Double]("sales", Some(14630732.52), None, 14630732.52, 100.0, isMatched = false)
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
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-11"), "java.sql.Date"),
						QueryColumn("count", Some(10409), "java.lang.Integer"),
						QueryColumn("sales", Some(14630732.52), "java.lang.Double")
					),
					"Technology|2021-06-11"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", Some(10821.0), None, 10821.0, 100.0, isMatched = false),
						NumberColumn[Double]("sales", Some(15630330.46), None, 15630330.46, 100.0, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-11"),
					),
					List(
						NumberColumn[Double]("count", Some(10409.0), None, 10409.0, 100.0, isMatched = false),
						NumberColumn[Double]("sales", Some(14630732.52), None, 14630732.52, 100.0, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", None, Some(10821.0), -10821.0, -100.0, isMatched = false),
						NumberColumn[Double]("sales", None, Some(15630330.46), -15630330.46, -100.0, isMatched = false)
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
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10065), "java.lang.Integer"),
						QueryColumn("sales", Some(15630320.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(47531), "java.lang.Integer"),
						QueryColumn("sales", Some(37680322.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(47532), "java.lang.Integer"),
						QueryColumn("sales", Some(37680362.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", Some(47531.0), Some(47532.0), -1.0, -0.0021038901, isMatched = false),
						NumberColumn[Double]("sales", Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562, isMatched = false),
						StringLikeColumn("audit_date", None, Some(sampleAuditDate), isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn[Double]("count", Some(10065.0), Some(10821.0), -756, -7.5111773472, isMatched = false),
						NumberColumn[Double]("sales", Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782, isMatched = false),
						StringLikeColumn("audit_date", None, Some(sampleAuditDate), isMatched = false)
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
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(10065), "java.lang.Integer"),
						QueryColumn("sales", Some(15630320.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("audit_date", Some(sampleAuditDate), "java.util.Date"),
						QueryColumn("count", Some(47531), "java.lang.Integer"),
						QueryColumn("sales", Some(37680322.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(10821), "java.lang.Integer"),
						QueryColumn("sales", Some(15630330.46), "java.lang.Double")
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(47532), "java.lang.Integer"),
						QueryColumn("sales", Some(37680362.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)


			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						StringLikeColumn("audit_date", Some(sampleAuditDate), None, isMatched = false),
						NumberColumn[Double]("count", Some(47531.0), Some(47532.0), -1.0, -0.0021038901, isMatched = false),
						NumberColumn[Double]("sales", Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						StringLikeColumn("audit_date", Some(sampleAuditDate), None, isMatched = false),
						NumberColumn[Double]("count", Some(10065.0), Some(10821.0), -756, -7.5111773472, isMatched = false),
						NumberColumn[Double]("sales", Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782, isMatched = false)
					)
				)
			)

			(Some(sourceRecords), Some(targetRecords), prepareQuery, expectedReconciliation)
		}
	}

	object EmptyResult {}
	object TypedValue {}
}
