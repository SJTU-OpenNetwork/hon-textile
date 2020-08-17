package mobile

// ConnectShadowTCP connect with shadow peer through tcp directly.
// It would not through any error. Only return connect success or not.
func (m *Mobile) ConnectShadowTCP(ip string, port int) bool {
	err := m.node.ConnectShadowTCP(ip, port)
	if err != nil {
		log.Error("Connect shadow failed: ", err)
		return false
	}
	return true
}

// Async version of ConnectShadowTCP
func (m *Mobile) ConnectShadowTCP_Async(ip string, port int, cb Callback) {
	m.node.WaitAdd(1, "Mobile.ConnectShadow")
	go func() {
		defer m.node.WaitDone("Mobile.ConnectShadow")
		//cb.Call(m.dataAtPath(pth))
		cb.Call(m.node.ConnectShadowTCP(ip, port))
	}()
}
