package blobs2gzip

func ComposeErr[T, U, V any](
	f func(T) (U, error),
	g func(U) (V, error),
) func(T) (V, error) {
	return func(t T) (v V, e error) {
		u, e := f(t)
		switch e {
		case nil:
			return g(u)
		default:
			return v, e
		}
	}
}

func Compose[T, U, V any](
	f func(T) U,
	g func(U) V,
) func(T) V {
	return func(t T) V {
		var u U = f(t)
		return g(u)
	}
}
