package filter

import (
	"fmt"

	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/scheduler/node"
	"github.com/docker/docker/nat"
	"github.com/samalba/dockerclient"
)

// PortFilter guarantees that, when scheduling a container binding a public
// port, only nodes that have not already allocated that same port will be
// considered.
type PortFilter struct {
}

// Name returns the name of the filter
func (p *PortFilter) Name() string {
	return "port"
}

// Filter is exported
func (p *PortFilter) Filter(config *cluster.ContainerConfig, nodes []*node.Node) ([]*node.Node, error) {
	if config.HostConfig.NetworkMode == "host" {
		return p.filterHost(config, nodes)
	}

	return p.filterBridge(config, nodes)
}

func (p *PortFilter) filterHost(config *cluster.ContainerConfig, nodes []*node.Node) ([]*node.Node, error) {
	for port := range config.ExposedPorts {
		candidates := []*node.Node{}
		for _, node := range nodes {
			if !p.portAlreadyExposed(node, port) {
				candidates = append(candidates, node)
			}
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("unable to find a node with port %s available in the Host mode", port)
		}
		nodes = candidates
	}
	return nodes, nil
}

func (p *PortFilter) filterBridge(config *cluster.ContainerConfig, nodes []*node.Node) ([]*node.Node, error) {
	for _, port := range config.HostConfig.PortBindings {
		for _, binding := range port {
			requestedStart, requestedEnd, err := nat.ParsePortRange(binding.HostPort)
			if err != nil {
				return nil, err
			}
			candidates := []*node.Node{}
			for _, node := range nodes {
				if in_use, err := p.portAlreadyInUse(node, binding.HostIp, requestedStart, requestedEnd); err != nil {
					return nil, err
				} else if !in_use {
					candidates = append(candidates, node)
				}
			}
			if len(candidates) == 0 {
				return nil, fmt.Errorf("unable to find a node with port %s available", binding.HostPort)
			}
			nodes = candidates
		}
	}
	return nodes, nil
}

func (p *PortFilter) portAlreadyExposed(node *node.Node, requestedPort string) bool {
	for _, c := range node.Containers {
		if c.Info.HostConfig.NetworkMode == "host" {
			for port := range c.Info.Config.ExposedPorts {
				if port == requestedPort {
					return true
				}
			}
		}
	}
	return false
}

func (p *PortFilter) portAlreadyInUse(node *node.Node, requestedIp string, requestedStart, requestedEnd int) (bool, error) {
	if requestedStart == 0 && requestedEnd == 0 {
		return false, nil
	}
	portsInUse := make(map[int]interface{})
	for _, c := range node.Containers {
		// HostConfig.PortBindings contains the requested ports.
		// NetworkSettings.Ports contains the actual ports.
		//
		// We have to check both because:
		// 1/ If the port was not specifically bound (e.g. -p 80), then
		//    HostConfig.PortBindings.HostPort will be empty and we have to check
		//    NetworkSettings.Port.HostPort to find out which port got dynamically
		//    allocated.
		// 2/ If the port was bound (e.g. -p 80:80) but the container is stopped,
		//    NetworkSettings.Port will be null and we have to check
		//    HostConfig.PortBindings to find out the mapping.

		if all_in_use, err := p.compare(portsInUse, requestedIp, requestedStart, requestedEnd, c.Info.HostConfig.PortBindings); err != nil {
			return false, err
		} else if all_in_use {
			return true, nil
		}
		if all_in_use, err := p.compare(portsInUse, requestedIp, requestedStart, requestedEnd, c.Info.NetworkSettings.Ports); err != nil {
			return false, err
		} else if all_in_use {
			return true, nil
		}
	}
	return false, nil
}

func (p *PortFilter) compare(portsInUse map[int]interface{}, requestedIp string, requestedStart, requestedEnd int, bindings map[string][]dockerclient.PortBinding) (bool, error) {
	for _, binding := range bindings {
		for _, b := range binding {
			bindingStart, bindingEnd, err := nat.ParsePortRange(b.HostPort)
			if err != nil {
				return false, err
			}
			if (bindingStart == 0 && bindingEnd == 0) || (bindingStart != bindingEnd) {
				// Skip undefined HostPorts. This happens in bindings that
				// didn't explicitely specify an external port.
				// Also skip HostPort ranges; rely only on NetworkSettings.Port for this case.
				continue
			}

			if bindingStart >= requestedStart && bindingStart <= requestedEnd {
				// Another container on the same host is binding in the same
				// port/protocol range.  Verify if they are requesting the same
				// binding IP, or if the other container is already binding on
				// every interface.
				if requestedIp == b.HostIp || bindsAllInterfaces(requestedIp) || bindsAllInterfaces(b.HostIp) {
					portsInUse[bindingStart] = true
					if len(portsInUse) >= (requestedEnd - requestedStart + 1) {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

func bindsAllInterfaces(hostIp string) bool {
	return hostIp == "0.0.0.0" || hostIp == ""
}
