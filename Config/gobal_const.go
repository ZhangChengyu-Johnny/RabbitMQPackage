package Config

const (
	MQIP       = "192.168.31.64"
	MQPORT     = "5672"
	MQUSER     = "admin"
	MQPASSWORD = "admin123456"
	MQVHOST    = "go-test"
	MQDNS      = "amqp://" + MQUSER + ":" + MQPASSWORD + "@" + MQIP + ":" + MQPORT + "/" + MQVHOST
)
