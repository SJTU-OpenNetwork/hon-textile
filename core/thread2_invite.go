package core

import "github.com/SJTU-OpenNetwork/hon-textile/pb"

// Invite peer to a thread
func (t *Textile) Invite(threadID string, peerID string) error {
	// create invite envelope

	// send the envelope to peer through t.mail.SendMessage(peerID, envelope)

	return nil
}

// Handle invite, called by watchMailbox
func (t *Textile) handleInvite(env *pb.Envelope) error {
	// join the thread

	return nil
}
