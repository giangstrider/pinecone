package reconciliation

import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationRecord}


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
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10821)),
						QueryColumn("sales", Some(15630330.46))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(47532)),
						QueryColumn("sales", Some(37680362.15))
					),
					"Fashion|2021-06-10"
				)
			)
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10065)),
						QueryColumn("sales", Some(15630320.46))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(47531)),
						QueryColumn("sales", Some(37680322.15))
					),
					"Fashion|2021-06-10"
				)
			)

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					"Fashion|2021-06-10",
					List(
						ReconcileColumn("count", Some(47532.0), Some(47531.0), 1.0, 0.0021038458),
						ReconcileColumn("sales", Some(37680362.15), Some(37680322.15), 40.0, 0.0001061561)
					)
				),
				ReconciliationRecord(
					queryKey,
					"Technology|2021-06-10",
					List(
						ReconcileColumn("count", Some(10821.0), Some(10065.0), 756, 6.9864153036),
						ReconcileColumn("sales", Some(15630330.46), Some(15630320.46), 10.0, 0.0000639782)
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
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10065)),
						QueryColumn("sales", Some(15630320.46))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(47531)),
						QueryColumn("sales", Some(37680322.15))
					),
					"Fashion|2021-06-10"
				)
			)
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10821)),
						QueryColumn("sales", Some(15630330.46))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Fashion")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(47532)),
						QueryColumn("sales", Some(37680362.15))
					),
					"Fashion|2021-06-10"
				)
			)

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					"Fashion|2021-06-10",
					List(
						ReconcileColumn("count", Some(47531.0), Some(47532.0), -1.0, -0.0021038901),
						ReconcileColumn("sales", Some(37680322.15), Some(37680362.15), -40.0, -0.0001061562)
					)
				),
				ReconciliationRecord(
					queryKey,
					"Technology|2021-06-10",
					List(
						ReconcileColumn("count", Some(10065.0), Some(10821.0), -756, -7.5111773472),
						ReconcileColumn("sales", Some(15630320.46), Some(15630330.46), -10.0, -0.0000639782)
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
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10821)),
						QueryColumn("sales", Some(15630330.46))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-11")),
						QueryColumn("count", Some(10409)),
						QueryColumn("sales", Some(14630732.52))
					),
					"Technology|2021-06-11"
				)
			)
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10821)),
						QueryColumn("sales", Some(15630330.46))
					),
					"Technology|2021-06-10"
				)
			)

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					"Technology|2021-06-10",
					List(
						ReconcileColumn("count", Some(10821.0), Some(10821.0), 0.0, 0.0),
						ReconcileColumn("sales", Some(15630330.46), Some(15630330.46), 0.0, 0.0)
					)
				),
				ReconciliationRecord(
					queryKey,
					"Technology|2021-06-11",
					List(
						ReconcileColumn("count", Some(10409.0), Some(0), 10409.0, 100.0),
						ReconcileColumn("sales", Some(14630732.52), Some(0), 14630732.52, 100.0)
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
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10821)),
						QueryColumn("sales", Some(15630330.46))
					),
					"Technology|2021-06-10"
				),
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-11")),
						QueryColumn("count", Some(10409)),
						QueryColumn("sales", Some(14630732.52))
					),
					"Technology|2021-06-11"
				)
			)
			val sourceQuery = ExecutedQuery(queryKey, sourceRecords, false)

			val targetRecords = List(
				QueryRecord(
					List(
						QueryColumn("department", Some("Technology")),
						QueryColumn("transaction_date", Some("2021-06-10")),
						QueryColumn("count", Some(10821)),
						QueryColumn("sales", Some(15630330.46))
					),
					"Fashion|2021-06-10"
				)
			)

			val targetQuery = ExecutedQuery(queryKey, targetRecords, true)

			val expectedReconciliation = List(
				ReconciliationRecord(
					queryKey,
					"Technology|2021-06-10",
					List(
						ReconcileColumn("count", Some(10821), Some(0), 10821.0, 100.0),
						ReconcileColumn("sales", Some(15630330.46), Some(0), 15630330.46, 100.0)
					)
				),
				ReconciliationRecord(
					queryKey,
					"Technology|2021-06-11",
					List(
						ReconcileColumn("count", Some(10409), Some(0), 10409.0, 100.0),
						ReconcileColumn("sales", Some(14630732.52), Some(0), 14630732.52, 100.0)
					)
				),
				ReconciliationRecord(
					queryKey,
					"Fashion|2021-06-10",
					List(
						ReconcileColumn("count", Some(0), Some(10821), -10821.0, -100.0),
						ReconcileColumn("sales", Some(0), Some(15630330.46), -15630330.46, -100.0)
					)
				)
			)

			(sourceQuery, targetQuery, prepareQuery, expectedReconciliation)
		}
	}
}
