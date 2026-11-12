// Package apperr is a small coded error used by the lab.
package apperr

import "errors"

type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Message }

func New(code, message string, status int) *Error {
	return &Error{Code: code, Message: message, Status: status}
}

func As(err error) (*Error, bool) {
	var item *Error
	if errors.As(err, &item) {
		return item, true
	}
	return nil, false
}
