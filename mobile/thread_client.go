package mobile

import (
)

func (m *Mobile) CreateGroup() (string,error) {
	threadid, err := m.node.CreateGroup()
	if err != nil{
		return "",err
	}
	threadIdStr := threadid.String()
	return threadIdStr,nil
}

func (m *Mobile) CreateDB() (string,error) {
	threadid, err := m.node.CreateDB()
	if err != nil{
		return "",err
	}
	threadIdStr := threadid.String()
	return threadIdStr,nil
}

//func (m *Mobile) ListDBs() ([]byte,error) {
//	dblist,err := m.node.ListDBs()
//	if err != nil{
//		return nil,err
//	}
//
//}