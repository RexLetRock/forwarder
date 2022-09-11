package forwarder

const NCpu = 12
const NRun = 5_000_000
const ENDLINE = "#\t#"

const CSendSize = 1
const ChansSize = 1024

const TimeToDelay = 1
const ThreadPerConn = 5
const CCountSize = 1

type MSG struct {
	A string
	B []byte
}
