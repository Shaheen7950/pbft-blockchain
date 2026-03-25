package consensus

import "fmt"

var PublicKeys = map[string]int64{}
var PrivateKeys = map[string]int64{}

// Auto-generate keys for 15 nodes
func init() {
	for i := 1; i <= 15; i++ {
		id := fmt.Sprintf("%d", i)
		PublicKeys[id] = int64(1000 + i)
		PrivateKeys[id] = int64(1000 + i)
	}
}

func simpleHash(data string) int64 {
	var h int64 = 0
	for _, c := range data {
		h += int64(c)
	}
	return h
}

func Sign(nodeID string, message string) int64 {
	return PrivateKeys[nodeID] * simpleHash(message)
}

func VerifySingle(nodeID string, message string, sig int64) bool {
	return sig == PublicKeys[nodeID]*simpleHash(message)
}

func AggregateSignatures(signatures []int64) int64 {
	var agg int64 = 0
	for _, s := range signatures {
		agg += s
	}
	return agg
}

func VerifyAggregated(signers []string, message string, aggSig int64) bool {
	var expectedSum int64 = 0
	for _, id := range signers {
		expectedSum += PublicKeys[id]
	}
	return aggSig == (expectedSum * simpleHash(message))
}
