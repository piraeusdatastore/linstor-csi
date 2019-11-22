// Code generated by "enumer -type=paramKey"; DO NOT EDIT.

//
package volume

import (
	"fmt"
)

const _paramKeyName = "unknownallowremotevolumeaccessautoplaceclientlistdisklessonremainingdisklessstoragepooldonotplacewithregexencryptionfsoptslayerlistmountoptsnodelistplacementcountplacementpolicyreplicasondifferentreplicasonsamesizekibstoragepoolpostmountxfsoptsfstype"

var _paramKeyIndex = [...]uint8{0, 7, 30, 39, 49, 68, 87, 106, 116, 122, 131, 140, 148, 162, 177, 196, 210, 217, 228, 244, 250}

func (i paramKey) String() string {
	if i < 0 || i >= paramKey(len(_paramKeyIndex)-1) {
		return fmt.Sprintf("paramKey(%d)", i)
	}
	return _paramKeyName[_paramKeyIndex[i]:_paramKeyIndex[i+1]]
}

var _paramKeyValues = []paramKey{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}

var _paramKeyNameToValueMap = map[string]paramKey{
	_paramKeyName[0:7]:     0,
	_paramKeyName[7:30]:    1,
	_paramKeyName[30:39]:   2,
	_paramKeyName[39:49]:   3,
	_paramKeyName[49:68]:   4,
	_paramKeyName[68:87]:   5,
	_paramKeyName[87:106]:  6,
	_paramKeyName[106:116]: 7,
	_paramKeyName[116:122]: 8,
	_paramKeyName[122:131]: 9,
	_paramKeyName[131:140]: 10,
	_paramKeyName[140:148]: 11,
	_paramKeyName[148:162]: 12,
	_paramKeyName[162:177]: 13,
	_paramKeyName[177:196]: 14,
	_paramKeyName[196:210]: 15,
	_paramKeyName[210:217]: 16,
	_paramKeyName[217:228]: 17,
	_paramKeyName[228:244]: 18,
	_paramKeyName[244:250]: 19,
}

// paramKeyString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func paramKeyString(s string) (paramKey, error) {
	if val, ok := _paramKeyNameToValueMap[s]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to paramKey values", s)
}

// paramKeyValues returns all values of the enum
func paramKeyValues() []paramKey {
	return _paramKeyValues
}

// IsAparamKey returns "true" if the value is listed in the enum definition. "false" otherwise
func (i paramKey) IsAparamKey() bool {
	for _, v := range _paramKeyValues {
		if i == v {
			return true
		}
	}
	return false
}
