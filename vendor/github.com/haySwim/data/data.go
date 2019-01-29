/*
Data. Easy-ish storage unit utility.
Copyright © 2019 Hayley Swimelar

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

package data

import (
	"fmt"
	"math"
)

type ByteSize int64

const (
	B            = iota + 1
	KiB ByteSize = 1 << (10 * iota)
	MiB
	GiB
	TiB
	PiB
	EiB
)

type Amount interface {
	InclusiveBytes() ByteSize
	Value() float64
	To(ByteSize) float64
	fmt.Stringer
}

type Byte struct {
	bytes ByteSize
}

func NewByte(b ByteSize) Byte {
	return Byte{b}
}

func (b Byte) InclusiveBytes() ByteSize {
	return b.inclusiveBytes(B)
}

func (b Byte) Value() float64 {
	return float64(b.bytes)
}

func (b Byte) String() string {
	return b.toHuman()
}

func (b Byte) inclusiveBytes(base ByteSize) ByteSize {

	res := b.bytes / base
	remainder := b.bytes % base
	if remainder > 0 {
		res++
	}
	if remainder < 0 {
		res--
	}

	return res * base
}

func (b Byte) To(base ByteSize) float64 {
	return float64(b.bytes) / float64(base)
}

func (b Byte) toHuman() string {

	// Save signedness, do caclulations on absolute value of bytes.
	var sign string
	bytes := float64(b.bytes)
	if b.bytes < 0 {
		bytes = -bytes
		sign = "-"
	}

	sizes := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"}
	unit := float64(1024)

	// Too few bytes to meaningfully convert.
	if bytes < unit {
		return fmt.Sprintf("%s%.1f %s", sign, bytes, sizes[0])
	}

	exp := int(math.Log(bytes) / math.Log(unit))
	return fmt.Sprintf("%s%.1f %s", sign, (bytes / (math.Pow(unit, float64(exp)))), sizes[exp])
}

type KibiByte struct {
	bytes Byte
}

func NewKibiByte(b ByteSize) KibiByte {
	return KibiByte{Byte{b}}
}

func (k KibiByte) InclusiveBytes() ByteSize {
	return k.bytes.inclusiveBytes(KiB)
}

func (k KibiByte) Value() float64 {
	return float64(k.bytes.bytes) / float64(KiB)
}

func (k KibiByte) String() string {
	return fmt.Sprintf(k.bytes.toHuman())
}

func (k KibiByte) To(base ByteSize) float64 {
	return float64(k.bytes.bytes) / float64(base)
}
