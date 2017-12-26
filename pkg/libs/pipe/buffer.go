package pipe

type Buffer interface {
	ReadSome(b []byte) (int, error)
	Buffered() int
	CloseReader() error

	WriteSome(b []byte) (int, error)
	Available() int
	CloseWriter() error
}
