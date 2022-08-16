package def

type Consensus interface {
	Name() string
	Producer() Producer
	User() User
	//SnapshotSender() chaindef.SnapshotSender
	//SnapshotReceiver() chaindef.SnapshotReceiver
	SetProducer(p Producer)
	SetUser(u User)
	//SetSnapshotSender(sss chaindef.SnapshotSender)
	//SetSnapshotReceiver(ssr chaindef.SnapshotReceiver)
}
