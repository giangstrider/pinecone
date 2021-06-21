package reconciliation

import org.scalatest.flatspec.AnyFlatSpec
import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationRecord}


class ReconciliationUnitTest extends AnyFlatSpec {
	private def reconcileTest(sampleData: (ExecutedQuery, ExecutedQuery, PrepareQuery, List[ReconciliationRecord])) = {
		val (sourceQuery, targetQuery, prepareQuery, expectedReconciliation) = sampleData
		val results = Reconciliation.reconcile(sourceQuery, targetQuery, prepareQuery)
		assert(expectedReconciliation === results)
	}

	"A matched single pair" should "return POSITIVE when SOURCE GREATER than TARGET" in {
		reconcileTest(ReconciliationSampleData.SingleMatchQueryKey.sourceGreaterThanTarget)
	}

	it should "return NEGATIVE when TARGET GREATER than SOURCE" in {
		reconcileTest(ReconciliationSampleData.SingleMatchQueryKey.targetGreaterThanSource)
	}

	it should "return 100% deviation when TARGET missing ReconcileKey compare to SOURCE" in {
		reconcileTest(ReconciliationSampleData.SingleMatchQueryKey.targetMissingReconcileKeyOfSource)
	}

	it should "return 100% deviation when for both side which miss keys from each other" in {
		reconcileTest(ReconciliationSampleData.SingleMatchQueryKey.bothMissingReconcileKeyOfEachOther)
	}
}
