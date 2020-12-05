package failover

import (
	"fmt"
	"net"

	. "github.com/haidaochuan1123/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// MysqlGTIDHandler MySQL gtid模式下切换实现
type MysqlGTIDHandler struct {
	Handler
}

// Promote 提升为主库
func (h *MysqlGTIDHandler) Promote(s *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return errors.Trace(err)
	}

	if err := s.StopSlave(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// FindBestSlaves 对比binlog位点，选择最近的节点做为主库
func (h *MysqlGTIDHandler) FindBestSlaves(slaves []*Server) ([]*Server, error) {
	// MHA use Relay_Master_Log_File and Exec_Master_Log_Pos to determind which is the best slave

	bestSlaves := []*Server{}

	ps := make([]Position, len(slaves))

	lastIndex := -1

	for i, slave := range slaves {
		pos, err := slave.FetchSlaveExecutePos()

		if err != nil {
			return nil, errors.Trace(err)
		}

		ps[i] = pos

		if lastIndex == -1 {
			lastIndex = i
			bestSlaves = []*Server{slave}
		} else {
			switch ps[lastIndex].Compare(pos) {
			case 1:
				//do nothing
			case -1:
				lastIndex = i
				bestSlaves = []*Server{slave}
			case 0:
				// these two slaves have same data,
				bestSlaves = append(bestSlaves, slave)
			}
		}
	}

	return bestSlaves, nil
}

const changeMasterToWithAuto = `CHANGE MASTER TO 
    MASTER_HOST = "%s", MASTER_PORT = %s, 
    MASTER_USER = "%s", MASTER_PASSWORD = "%s", 
    MASTER_AUTO_POSITION = 1`

// ChangeMasterTo 修改其它从节点的主库信息
func (h *MysqlGTIDHandler) ChangeMasterTo(s *Server, m *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return errors.Trace(err)
	}

	if err := s.StopSlave(); err != nil {
		return errors.Trace(err)
	}

	if err := s.ResetSlave(); err != nil {
		return errors.Trace(err)
	}

	host, port, _ := net.SplitHostPort(m.Addr)

	if _, err := s.Execute(fmt.Sprintf(changeMasterToWithAuto,
		host, port, m.ReplUser.Name, m.ReplUser.Password)); err != nil {
		return errors.Trace(err)
	}

	if err := s.StartSlave(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// WaitRelayLogDone 等待relaylog 指定完毕
func (h *MysqlGTIDHandler) WaitRelayLogDone(s *Server) error {
	if err := s.StopSlaveIOThread(); err != nil {
		return errors.Trace(err)
	}

	r, err := s.SlaveStatus()
	if err != nil {
		return errors.Trace(err)
	}

	retrieved, _ := r.GetStringByName(0, "Retrieved_Gtid_Set")

	// may only support MySQL version >= 5.6.9
	// see http://dev.mysql.com/doc/refman/5.6/en/gtid-functions.html
	return h.waitUntilAfterGTIDs(s, retrieved)
}

// WaitCatchMaster 等待追上主库
func (h *MysqlGTIDHandler) WaitCatchMaster(s *Server, m *Server) error {
	r, err := m.MasterStatus()
	if err != nil {
		return errors.Trace(err)
	}

	masterGTIDSet, _ := r.GetStringByName(0, "Executed_Gtid_Set")

	return h.waitUntilAfterGTIDs(s, masterGTIDSet)
}

// CheckGTIDMode 判断是否开启gtid
func (h *MysqlGTIDHandler) CheckGTIDMode(slaves []*Server) error {
	for i := 0; i < len(slaves); i++ {
		mode, err := slaves[i].MysqlGTIDMode()
		if err != nil {
			return errors.Trace(err)
		} else if mode != GTIDModeOn {
			return errors.Errorf("%s use not GTID mode", slaves[i].Addr)
		}
	}

	return nil
}

func (h *MysqlGTIDHandler) waitUntilAfterGTIDs(s *Server, gtids string) error {
	_, err := s.Execute(fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s')", gtids))
	return err
}
