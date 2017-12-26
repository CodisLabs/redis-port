package pipe

type Pipe struct {
}

func (p *Pipe) Read(b []byte) (int, error) {
	panic("TODO")
}

func (p *Pipe) Buffered() (int, error) {
	panic("TODO")
}

func (p *Pipe) CloseReader(err error) error {
	panic("TODO")
}

func (p *Pipe) Write(b []byte) (int, error) {
	panic("TODO")
}

func (p *Pipe) Available() (int, error) {
	panic("TODO")
}

func (p *Pipe) CloseWriter(err error) error {
	panic("TODO")
}
