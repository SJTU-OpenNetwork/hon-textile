package cmd

import (
	"net/http"
)

//group operations
func ThreadClientAddGroup(groupName string) error {
	cmdOpt := map[string]string{"groupName":groupName}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/add1", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddGroup2(groupName string) error {
	cmdOpt := map[string]string{"groupName":groupName}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/add2", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddGroupFromToken(threadId string, addr string, key string) error {
	cmdOpt := map[string]string{"threadId":threadId,"addr":addr,"key":key}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/add3", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddGroupFromToken1(threadName string) error {
	cmdOpt := map[string]string{"threadName":threadName}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/fromtoken1", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddGroupFromToken2(threadId string, token string) error {
	cmdOpt := map[string]string{"threadId":threadId,"token":token}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/fromtoken2", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientListDB() error {
	res, err := executeJsonCmd(http.MethodPost, "threadClient/group/ls", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientGroupName(threadId string) error {
	cmdOpt := map[string]string{"threadId": threadId}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/name", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientGroupInfoMod(threadId string, name string) error {
	cmdOpt := map[string]string{"threadId": threadId,"name": name}
	res, err := executeStringCmd(http.MethodPost, "threadClient/group/newName", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

//group message operations
func ThreadClientAddString(threadId string, text string) error {
	cmdOpt := map[string]string{"threadId": threadId, "text": text}
	res, err := executeStringCmd(http.MethodPut, "threadClient/message/add", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

//
func ThreadClientRemoveMessage(threadId string, instanceId string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": instanceId}
	res, err := executeStringCmd(http.MethodPut, "threadClient/message/del", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientFindMessage(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/message/get", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

//thread peer operations
func ThreadClientAddPeer(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "peerId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/peer/add", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientRemovePeer(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/peer/remove", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientModiPeer(threadId string, pid string, role string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid, "role": role}
	res, err := executeStringCmd(http.MethodPost, "threadClient/peer/set", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientFindPeer(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/peer/role", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

