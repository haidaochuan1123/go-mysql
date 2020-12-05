package mysql

// Result 执行返回信息
type Result struct {
	Status uint16

	InsertID     uint64
	AffectedRows uint64

	*Resultset
}

// Executer SQL执行接口
type Executer interface {
	Execute(query string, args ...interface{}) (*Result, error)
}

// Close 关闭执行结果
func (r *Result) Close() {
	if r.Resultset != nil {
		r.Resultset.returnToPool()
		r.Resultset = nil
	}
}
