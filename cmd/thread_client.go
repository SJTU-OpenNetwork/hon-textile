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

func ThreadClientListDB() error {
	res, err := executeJsonCmd(http.MethodPost, "threadClient/listDB", params{}, nil)
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientAddString(threadId string, text string) error {
	res, err := executeJsonCmd(http.MethodPut, "threadClient/addString/"+threadId, params{
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

func ThreadClientGroupName(threadId string, instanceId string) error {
	cmdOpt := map[string]string{"threadId": threadId, "instanceId":instanceId}
	res, err := executeStringCmd(http.MethodPost, "threadClient/groupInfo", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}

func ThreadClientGroupInfoMod(threadId string, instanceId string, name string) error {
	cmdOpt := map[string]string{"threadId": threadId,"instanceId":instanceId,"name": name}
	res, err := executeStringCmd(http.MethodPost, "threadClient/newGroupName", params{opts:cmdOpt})
	if err != nil {
		return err
	}
	output(res)
	return nil
}