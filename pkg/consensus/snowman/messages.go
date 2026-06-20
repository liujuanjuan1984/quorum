package snowman

import (
	"encoding/base64"
	"fmt"
	"time"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	localcrypto "github.com/rumsystem/quorum/pkg/crypto"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
	"google.golang.org/protobuf/proto"
)

func NewMessage(groupID string, typ quorumpb.SnowmanMessageType, requestID string, senderPubkey string) *quorumpb.SnowmanMessage {
	return &quorumpb.SnowmanMessage{
		GroupId:      groupID,
		Type:         typ,
		RequestId:    requestID,
		SenderPubkey: senderPubkey,
		Timestamp:    time.Now().UnixNano(),
	}
}

func SignMessage(msg *quorumpb.SnowmanMessage, nodename string) error {
	if msg == nil {
		return fmt.Errorf("snowman message is nil")
	}
	msg.Signature = nil
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	hash := localcrypto.Hash(payload)
	signature, err := localcrypto.GetKeystore().EthSignByKeyName(msg.GroupId, hash, nodename)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyMessageSignature(msg *quorumpb.SnowmanMessage) error {
	if msg == nil {
		return fmt.Errorf("snowman message is nil")
	}
	if len(msg.Signature) == 0 {
		return fmt.Errorf("snowman message signature is empty")
	}
	signature := append([]byte(nil), msg.Signature...)
	msg.Signature = nil
	payload, err := proto.Marshal(msg)
	msg.Signature = signature
	if err != nil {
		return err
	}
	hash := localcrypto.Hash(payload)
	pubkeyBytes, err := base64.RawURLEncoding.DecodeString(msg.SenderPubkey)
	if err != nil {
		return err
	}
	ethpubkey, err := ethcrypto.DecompressPubkey(pubkeyBytes)
	if err != nil {
		return err
	}
	if !localcrypto.GetKeystore().EthVerifySign(hash, signature, ethpubkey) {
		return fmt.Errorf("snowman message signature verification failed")
	}
	return nil
}
