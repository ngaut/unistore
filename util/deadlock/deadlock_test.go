package deadlock

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeadlock(t *testing.T) {
	detector := NewDetector()
	err := detector.Detect(1, 2, 100)
	require.Nil(t, err)
	err = detector.Detect(2, 3, 200)
	require.Nil(t, err)
	err = detector.Detect(3, 1, 300)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), fmt.Sprintf("deadlock(200)"))
	detector.CleanUp(2)
	list2 := detector.waitForMap[2]
	require.Nil(t, list2)

	// After cycle is broken, no deadlock now.
	err = detector.Detect(3, 1, 300)
	require.Nil(t, err)
	list3 := detector.waitForMap[3]
	require.Len(t, list3.txns, 1)

	// Different keyHash grows the list.
	err = detector.Detect(3, 1, 400)
	require.Nil(t, err)
	require.Len(t, list3.txns, 2)

	// Same waitFor and key hash doesn't grow the list.
	err = detector.Detect(3, 1, 400)
	require.Nil(t, err)
	require.Len(t, list3.txns, 2)

	detector.CleanUpWaitFor(3, 1, 300)
	require.Len(t, list3.txns, 1)
	detector.CleanUpWaitFor(3, 1, 400)
	list3 = detector.waitForMap[3]
	require.Nil(t, list3)
}
