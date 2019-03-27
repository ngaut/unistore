package util

import "container/list"

/// MustConsumerVec is a list with a tag.
type MustConsumerVec struct {
	tag string
	v   *list.List
}
