package RabbitMQPackage

const (
	MQIP       = "192.168.31.64"
	MQPORT     = "5672"
	MQUSER     = "admin"
	MQPASSWORD = "admin123456"
	MQVHOST    = "go-test"
	MQDNS      = "amqp://" + MQUSER + ":" + MQPASSWORD + "@" + MQIP + ":" + MQPORT + "/" + MQVHOST
)

type Mode string

var (
	SimpleMode       Mode = "simple"
	WorkMode         Mode = "work"
	ConfirmMode      Mode = "confirm"
	RoutingMode      Mode = "routing"
	SubscriptionMode Mode = "subscription"
	TopicMode        Mode = "topic"
	TTLMode          Mode = "ttl"
)
