package kvs

type chanPeer struct {
	id     string
	input  chan Req
	output chan Req
	quit   chan struct{}
	done   chan struct{}
}

func (p *chanPeer) ID() string {
	return p.id
}

func (p *chanPeer) Input() chan<- Req {
	return p.input
}

func (p *chanPeer) start() {
	responses := make(chan Res)
	go func() {
		for {
			select {
			case r := <-p.input:
				r1 := Req{
					Msg:      r.Msg,
					Response: responses,
				}
				p.output <- r1
				res := <-responses
				r.Response <- Res{
					PeerID: p.id,
					Msg:    res.Msg,
					Req:    &r,
				}
			case <-p.quit:
				close(p.done)
				return
			}
		}
	}()
}

func startCP(node *Node) *chanPeer {
	p := &chanPeer{
		id:     node.id,
		input:  make(chan Req),
		output: node.input,
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	p.start()
	return p
}
