package redis

import (
	"fmt"
	"log"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	redigo "github.com/garyburd/redigo/redis"
	cmap "github.com/orcaman/concurrent-map"
)

var (
	Pool        redis
	hosts       cmap.ConcurrentMap
	breakerCmap cmap.ConcurrentMap
)

// Options configuration options for redis connection
type Options struct {
	MaxIdleConn   int
	MaxActiveConn int
	Timeout       int
	Wait          bool
}

// config used when we need to open new connection automatically
type config struct {
	Address string
	Network string
	Option  Options
}

type redis struct {
	DBs cmap.ConcurrentMap
}

func init() {
	Pool.DBs = cmap.New()
	hosts = cmap.New()
	breakerCmap = cmap.New()
}
func New(hostName, hostAddress, network string, customOption ...Options) {
	opt := Options{
		MaxIdleConn:   10,
		MaxActiveConn: 1000,
		Timeout:       1,
		Wait:          false,
	}
	if len(customOption) > 0 {
		opt = customOption[0]
	}

	Pool.DBs.Set(hostName, &redigo.Pool{
		MaxIdle:     opt.MaxIdleConn,
		MaxActive:   opt.MaxActiveConn,
		IdleTimeout: time.Duration(opt.Timeout) * time.Second,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial(network, hostAddress)
			if err != nil {
				log.Println("[redis][New] error dial host:", err)
				return nil, err
			}
			return c, nil
		},
	})

	// save the connection address and options for later ue
	cfg := config{
		Address: hostAddress,
		Network: network,
		Option: Options{
			MaxActiveConn: opt.MaxActiveConn,
			MaxIdleConn:   opt.MaxIdleConn,
			Timeout:       opt.Timeout,
			Wait:          true,
		},
	}
	hosts.Set(hostName, cfg)
}
func (r *redis) getConnection(dbname string) redigo.Conn {
	var rdsConn redigo.Conn

	circuitbreaker, cbOk := breakerCmap.Get(dbname)
	if !cbOk {
		circuitbreaker = breaker.New(10, 2, 10*time.Second)
		breakerCmap.Set(dbname, circuitbreaker)
	}

	cb := circuitbreaker.(*breaker.Breaker)
	cbResult := cb.Run(func() error {
		placeholderPool, ok := r.DBs.Get(dbname)
		if !ok {
			return fmt.Errorf("[redis] %s - failed to retrieve redis connection map", dbname)
		}

		redisPool := placeholderPool.(*redigo.Pool)
		rdsConn = redisPool.Get()
		if _, err := rdsConn.Do("PING"); err == nil {
			return nil
		} else {
			log.Println("[redis] ping error:", err)
			rdsConn.Close() // just in case
		}

		log.Printf("[redis] %s - bad connection, closing and opening new one\n", dbname)
		hosttemp, ok := hosts.Get(dbname)
		if !ok {
			return fmt.Errorf("[redis] %s - failed to retrieve connection info", dbname)
		}

		host := hosttemp.(config)

		if err := redisPool.Close(); err != nil {
			log.Printf("[redis] %s - failed to close connection: %+v\n", dbname, err)
			return err
		}

		Pool.DBs.Remove(dbname)
		Pool.DBs.Set(dbname, &redigo.Pool{
			MaxIdle:     host.Option.MaxIdleConn,
			MaxActive:   host.Option.MaxActiveConn,
			IdleTimeout: time.Duration(host.Option.Timeout) * time.Second,
			Dial: func() (redigo.Conn, error) {
				c, err := redigo.Dial(host.Network, host.Address)
				if err != nil {
					log.Println("[redis][getConnection] error dial host:", err)
					return nil, err
				}
				return c, nil
			},
		})

		// return the asked datatype
		rdsTempConn, ok := r.DBs.Get(dbname)
		if !ok {
			return fmt.Errorf("[redis] %s - failed to retrieve newly created redis connection map", dbname)
		}

		redisPool = rdsTempConn.(*redigo.Pool)
		// if the newly open connection is having error than it need human intervention
		rdsConn = redisPool.Get()
		return nil
	})

	if cbResult == breaker.ErrBreakerOpen {
		log.Printf("[redis] redis circuitbreaker open, retrying later.. - %s\n", dbname)
	}

	return rdsConn
}

// GetConnection return redigo.Conn which enable developers to do redis command
// that is not commonly used (special case command, one that don't have wrapper function in this package. e.g: Exists)
func (r *redis) GetConnection(dbname string) redigo.Conn {
	return r.getConnection(dbname)
}

func (r *redis) SetexMultiple(dbname string, data map[string]string, expired int) (res map[string]string, err error) {
	res = make(map[string]string)
	expString := fmt.Sprintf("%d", expired)
	conn := r.getConnection(dbname)
	if conn == nil {
		return res, fmt.Errorf("Failed to obtain connection db %s key %+v", dbname, data)
	}
	defer conn.Close()
	for key, val := range data {
		conn.Send("SETEX", key, val, expString)
	}
	conn.Flush()
	for key, _ := range data {
		resultKey, err := redigo.String(conn.Receive())
		if err != nil {
			continue
		}
		res[key] = resultKey
	}
	return
}

func (r *redis) Setex(dbname, key, value string, expired int) (string, error) {
	conn := r.getConnection(dbname)
	if conn != nil {
		defer conn.Close()
		return redigo.String(conn.Do("SETEX", key, value, fmt.Sprintf("%d", expired)))
	}
	return "", fmt.Errorf("Failed to obtain connection db %s key %s", dbname, key)

}
