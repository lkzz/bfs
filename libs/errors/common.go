package errors

const (
	RetOK                 = 1
	RetServiceUnavailable = 65533
	RetParamErr           = 65534
	RetInternalErr        = 65535
	RetServiceTimeout     = 65536
	// needle
	RetNeedleExist = 5000
)

var (
	// common
	ErrParam              = Error(RetParamErr)
	ErrInternal           = Error(RetInternalErr)
	ErrServiceUnavailable = Error(RetServiceUnavailable)
	ErrServiceTimeout     = Error(RetServiceTimeout)

	ErrNeedleExist = Error(RetNeedleExist)
)
