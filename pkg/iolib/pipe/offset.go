package pipe

func align(n, unit int) int {
	if n < unit {
		return unit
	}
	return (n + unit - 1) / unit * unit
}

func roffset(n, size int, rpos, wpos uint64) (nn int, offset uint64) {
	if d := int(wpos - rpos); d < n {
		n = d
	}
	offset = rpos % uint64(size)
	if d := size - int(offset); d < n {
		n = d
	}
	return n, offset
}

func woffset(n, size int, rpos, wpos uint64) (nn int, offset uint64) {
	if d := size - int(wpos-rpos); d < n {
		n = d
	}
	offset = wpos % uint64(size)
	if d := size - int(offset); d < n {
		n = d
	}
	return n, offset
}
