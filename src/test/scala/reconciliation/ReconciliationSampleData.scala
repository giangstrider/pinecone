package reconciliation

import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationRecord}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}

object ReconciliationSampleData {
	object SingleMatchQueryKey {
		val sourceGreaterThanTarget = {
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
						QueryColumn("department", Some("Fashion"), "java.lang.String"),
						QueryColumn("transaction_date", Some("2021-06-10"), "java.sql.Date"),
						QueryColumn("count", Some(47532), "java.lang.Integer"),
						QueryColumn("sales", Some(37680362.15), "java.lang.Double")
					),
					"Fashion|2021-06-10"
				)
			)
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

			val targetRecords = List(
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

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 47532.0, 47531.0, 1.0, 0.0021038458, isMatched = false),
						NumberColumn("sales", 37680362.15, 37680322.15, 40.0, 0.0001061561, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 10821.0, 10065.0, 756, 6.9864153036, isMatched = false),
						NumberColumn("sales", 15630330.46, 15630320.46, 10.0, 0.0000639782, isMatched = false)
					)
				)
			)

			(sourceQuery, targetQuery, prepareQuery, expectedReconciliation)
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
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

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

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 47531.0, 47532.0, -1.0, -0.0021038901, isMatched = false),
						NumberColumn("sales", 37680322.15, 37680362.15, -40.0, -0.0001061562, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 10065.0, 10821.0, -756, -7.5111773472, isMatched = false),
						NumberColumn("sales", 15630320.46, 15630330.46, -10.0, -0.0000639782, isMatched = false)
					)
				)
			)

			(sourceQuery, targetQuery, prepareQuery, expectedReconciliation)
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
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

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

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 10821.0, 10821.0, 0.0, 0.0, isMatched = true),
						NumberColumn("sales", 15630330.46, 15630330.46, 0.0, 0.0, isMatched = true)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-11"),
					),
					List(
						NumberColumn("count", 10409.0, 0, 10409.0, 100.0, isMatched = false),
						NumberColumn("sales", 14630732.52, 0, 14630732.52, 100.0, isMatched = false)
					)
				)
			)

			(sourceQuery, targetQuery, prepareQuery, expectedReconciliation)
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
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

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

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 10821, 0, 10821.0, 100.0, isMatched = false),
						NumberColumn("sales", 15630330.46, 0, 15630330.46, 100.0, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Technology"),
						ReconcileKeyColumn("transaction_date", "2021-06-11"),
					),
					List(
						NumberColumn("count", 10409, 0, 10409.0, 100.0, isMatched = false),
						NumberColumn("sales", 14630732.52, 0, 14630732.52, 100.0, isMatched = false)
					)
				),
				ReconciliationRecord(
					queryKey,
					List(
						ReconcileKeyColumn("department", "Fashion"),
						ReconcileKeyColumn("transaction_date", "2021-06-10"),
					),
					List(
						NumberColumn("count", 0, 10821, -10821.0, -100.0, isMatched = false),
						NumberColumn("sales", 0, 15630330.46, -15630330.46, -100.0, isMatched = false)
					)
				)
			)

			(sourceQuery, targetQuery, prepareQuery, expectedReconciliation)
		}
	}
}
