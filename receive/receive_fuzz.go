//+build gofuzz

package receive

func Fuzz(data []byte) int {
	ts, err := parseWriteRequest(data)
	if err != nil {
		if ts != nil {
			panic("ts != nil on error")
		}
		return 0
	}
	return 1
}
