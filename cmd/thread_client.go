package cmd

import (
	"net/http"
	"strings"
)

func ThreadClientAddGroup(groupName string) error {
	cmdOpt := map[string]string{"groupName":groupName}
	res, err := executeStringCmd(http.MethodPost, "threadClient/addGroup", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddGroup2(groupName string) error {
	cmdOpt := map[string]string{"groupName":groupName}
	res, err := executeStringCmd(http.MethodPost, "threadClient/addGroup2", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddGroupFromToken(threadId string, addr string, key string) error {
	cmdOpt := map[string]string{"threadId":threadId,"addr":addr,"key":key}
	res, err := executeStringCmd(http.MethodPost, "threadClient/addGroup3", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientListDB() error {
	res, err := executeJsonCmd(http.MethodPost, "threadClient/listDB", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddString(threadId string, text string) error {
	res, err := executeJsonCmd(http.MethodPut, "threadClient/addMessage/"+threadId, params{
		payload: strings.NewReader(text)}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

//
func ThreadClientRemoveMessage(threadId string, instanceId string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": instanceId}
	res, err := executeStringCmd(http.MethodPut, "threadClient/delMessage", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientFindMessage(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/findMessage", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddPeer(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "peerId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/addPeer", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientRemovePeer(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/removePeer", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientModiPeer(threadId string, pid string, role string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid, "role": role}
	res, err := executeStringCmd(http.MethodPost, "threadClient/modPeer", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientFindPeer(threadId string, pid string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId": pid}
	res, err := executeStringCmd(http.MethodPost, "threadClient/findPeer", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientGroupName(threadId string) error {
	cmdOpt := map[string]string{"threadId": threadId}
	res, err := executeStringCmd(http.MethodPost, "threadClient/groupInfo", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientGroupInfoMod(threadId string, name string) error {
	cmdOpt := map[string]string{"threadId": threadId,"name": name}
	res, err := executeStringCmd(http.MethodPost, "threadClient/newGroupName", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}