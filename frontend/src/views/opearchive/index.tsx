import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/opearchive'
const columns = [
  '存档编号',
  '算子编号',
  '批次号',
  '调优记录编号',
  '生效并行度',
  '上一版本并行度',
  '生效状态',
  '批次版本校验',
  '未完成位置',
  '来源标识',
]
const rowActions = ['校验批次', '写入执行数据', '完成批次', '生效批次', '修正冲突', '作废批次']

type Row = Record<string, any>

interface WriteItem {
  序号: number
  项标识: string
  状态: string
  重试次数: number
  来源标识: string | null
  错误信息: string | null
}

export default function OpearchiveView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [detail, setDetail] = useState<Row | null>(null)
  const [conflicts, setConflicts] = useState<Row[]>([])
  const [snapshot, setSnapshot] = useState<Row | null>(null)
  const [snapshotQuery, setSnapshotQuery] = useState({ operator_no: 'OPER-0002', batch_no: '' })
  const [createForm, setCreateForm] = useState({ 算子编号: 'OPER-0002', 调优记录编号: 'PARA-0002', 批次号: '' })
  const [syncNode, setSyncNode] = useState('node-b')
  const [failInput, setFailInput] = useState<Record<string, string>>({})
  // 页面展示名 -> 后端查询参数名
  const filterFields = useMemo(() => [
    { label: '算子编号', param: 'operator_no' },
    { label: '批次号', param: 'batch_no' },
    { label: '状态', param: 'status' },
  ], [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(
      Object.entries(filters).filter(([, value]) => value.trim() !== ''),
    ).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) throw new Error('算子执行数据存档读取失败')
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '存档列表读取失败')
    }
  }, [filters])

  const reloadConflicts = useCallback(async () => {
    const response = await request(`${ENDPOINT}/conflicts`)
    if (response.ok) {
      const payload = await response.json()
      setConflicts(payload.items ?? [])
    }
  }, [])

  useEffect(() => {
    void reload()
    void reloadConflicts()
  }, [reload, reloadConflicts])

  async function runAction(action: string, row: Row) {
    setMessage('')
    setErrorMessage('')
    const values: Record<string, any> = { action }
    // 写入执行数据时可指定失败项序号，模拟批次失败后断点续传。
    if (action === '写入执行数据') {
      const raw = (failInput[String(row.id)] ?? '').trim()
      if (raw) {
        values.fail_positions = raw.split(/[,，]/).map((item) => Number(item.trim())).filter(Boolean)
      }
    }
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) throw new Error(payload.message || '动作未生效')
      setMessage(payload.message)
      await reload()
      await reloadConflicts()
      if (detail && detail.id === row.id) setDetail(payload.entry)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '存档操作失败')
    }
  }

  async function loadDetail(row: Row) {
    const response = await request(`${ENDPOINT}/${row.id}`)
    if (response.ok) setDetail(await response.json())
  }

  async function readSnapshot() {
    setMessage('')
    setErrorMessage('')
    const params = new URLSearchParams(
      Object.entries(snapshotQuery).filter(([, value]) => value.trim() !== ''),
    ).toString()
    try {
      const response = await request(`${ENDPOINT}/snapshot?${params}`)
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.detail || '执行快照读取失败')
      setSnapshot(payload)
    } catch (error) {
      setSnapshot(null)
      setErrorMessage(error instanceof Error ? error.message : '执行快照读取失败')
    }
  }

  async function createArchive() {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(ENDPOINT, {
        method: 'POST',
        body: JSON.stringify({ values: { ...createForm, 注册新批次: true } }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) throw new Error(payload.message || '批次存档登记失败')
      setMessage(payload.message)
      setCreateForm((prev) => ({ ...prev, 批次号: '' }))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '批次存档登记失败')
    }
  }

  async function syncConsumer(row: Row) {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/sync`, {
        method: 'POST',
        body: JSON.stringify({ values: { 存档编号: row.存档编号, 消费节点: syncNode } }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) throw new Error(payload.message || '跨系统同步失败')
      setMessage(payload.message)
      await reload()
      if (detail && detail.id === row.id) setDetail(payload.entry)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '跨系统同步失败')
    }
  }

  const writeItems: WriteItem[] = detail?.写入项 ?? []

  return (
    <section className="page" data-module="opearchive">
      <header className="page-head">
        <div>
          <h2>算子执行数据存档</h2>
          <p className="page-desc">
            并行度调优按批次边界生效：执行快照与调优记录读取同一批次，存档保留批次号与生效并行度；
            失败项可重试、中断保留未完成位置，跨系统同步引用同一份存档并保留来源标识。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出存档清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">存档总数</span>
          <strong className="stat-value">{total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">生效中批次</span>
          <strong className="stat-value">{rows.filter((row) => row.生效状态 === '生效中').length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">数据版本冲突</span>
          <strong className="stat-value">{conflicts.length}</strong>
        </article>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field.param} className="filter-item">
            <span>{field.label}</span>
            <input
              value={filters[field.param] ?? ''}
              placeholder={`按${field.label}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field.param]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>写入进度</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const items: WriteItem[] = row.写入项 ?? []
            const done = items.filter((item) => item.状态 === '已写入').length
            return (
              <tr key={String(row.id)}>
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td>{items.length ? `${done}/${items.length}` : '—'}</td>
                <td className="row-actions">
                  <button className="link" type="button" onClick={() => void loadDetail(row)}>查看明细</button>
                  {rowActions.map((action) => (
                    <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                      {action}
                    </button>
                  ))}
                  <button className="link" type="button" onClick={() => void syncConsumer(row)}>同步消费侧</button>
                </td>
              </tr>
            )
          })}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无算子执行数据存档，可在下方登记调优批次</td>
            </tr>
          )}
        </tbody>
      </table>

      {rows.some((row) => row.status === '失败') && (
        <div className="filter-bar">
          {rows.filter((row) => row.status === '失败').map((row) => (
            <label key={String(row.id)} className="filter-item">
              <span>{row.存档编号} 失败项序号（逗号分隔，留空=补写全部未完成项）</span>
              <input
                value={failInput[String(row.id)] ?? ''}
                placeholder="例如 2,3"
                onChange={(event) => setFailInput((prev) => ({ ...prev, [String(row.id)]: event.target.value }))}
              />
            </label>
          ))}
        </div>
      )}

      {detail && (
        <div className="stat-row" style={{ alignItems: 'flex-start' }}>
          <article className="stat-card" style={{ flex: 2 }}>
            <span className="stat-label">写入项明细（断点续传：已写入项不重写）</span>
            <table className="data-table" style={{ marginTop: 8 }}>
              <thead>
                <tr><th>序号</th><th>状态</th><th>重试次数</th><th>来源标识</th><th>错误信息</th></tr>
              </thead>
              <tbody>
                {writeItems.map((item) => (
                  <tr key={item.序号}>
                    <td>{item.序号}</td>
                    <td>{item.状态}</td>
                    <td>{item.重试次数}</td>
                    <td>{item.来源标识 ?? '—'}</td>
                    <td>{item.错误信息 ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </article>
          <article className="stat-card" style={{ flex: 1 }}>
            <span className="stat-label">跨系统同步记录（消费侧保留来源标识）</span>
            {(detail.同步记录 ?? []).length === 0 && <p className="page-desc">暂无同步记录</p>}
            {(detail.同步记录 ?? []).map((record: Row, index: number) => (
              <p key={index} className="page-desc">
                {record.消费节点} · {record.来源系统} · {record.来源标识} · {record.同步时间}
              </p>
            ))}
          </article>
        </div>
      )}

      <div className="stat-row" style={{ alignItems: 'flex-start' }}>
        <article className="stat-card" style={{ flex: 1 }}>
          <span className="stat-label">登记调优批次存档</span>
          {(['算子编号', '调优记录编号', '批次号'] as const).map((field) => (
            <label key={field} className="filter-item" style={{ display: 'flex', flexDirection: 'column', margin: '6px 0' }}>
              <span>{field}</span>
              <input
                value={createForm[field]}
                onChange={(event) => setCreateForm((prev) => ({ ...prev, [field]: event.target.value }))}
              />
            </label>
          ))}
          <button className="btn primary" type="button" onClick={() => void createArchive()}>登记批次存档</button>
        </article>

        <article className="stat-card" style={{ flex: 1 }}>
          <span className="stat-label">读取执行快照（只返回已校验批次）</span>
          {(['operator_no', 'batch_no'] as const).map((field) => (
            <label key={field} className="filter-item" style={{ display: 'flex', flexDirection: 'column', margin: '6px 0' }}>
              <span>{field === 'operator_no' ? '算子编号' : '批次号（留空读当前生效批次）'}</span>
              <input
                value={snapshotQuery[field]}
                onChange={(event) => setSnapshotQuery((prev) => ({ ...prev, [field]: event.target.value }))}
              />
            </label>
          ))}
          <button className="btn primary" type="button" onClick={() => void readSnapshot()}>读取快照</button>
          {snapshot && (
            <p className="page-desc" style={{ marginTop: 8 }}>
              批次 {snapshot.批次号} · 生效并行度 {snapshot.生效并行度} · 上一版本 {snapshot.上一版本并行度} ·{' '}
              校验 {snapshot.批次版本校验} · 来源 {snapshot.来源标识} · 写入进度 {JSON.stringify(snapshot.写入进度)}
            </p>
          )}
        </article>

        <article className="stat-card" style={{ flex: 1 }}>
          <span className="stat-label">跨系统同步</span>
          <label className="filter-item" style={{ display: 'flex', flexDirection: 'column', margin: '6px 0' }}>
            <span>消费节点</span>
            <input value={syncNode} onChange={(event) => setSyncNode(event.target.value)} />
          </label>
          <p className="page-desc">在列表行点击「同步消费侧」，未生效批次会被拒绝。</p>
        </article>
      </div>

      {conflicts.length > 0 && (
        <article className="stat-card" style={{ borderColor: '#d4380d' }}>
          <span className="stat-label">数据版本冲突（执行「修正冲突」收敛为单一终态）</span>
          <table className="data-table" style={{ marginTop: 8 }}>
            <thead>
              <tr><th>类型</th><th>算子编号</th><th>批次号</th><th>涉及记录</th><th>说明</th></tr>
            </thead>
            <tbody>
              {conflicts.map((conflict, index) => (
                <tr key={index}>
                  <td>{conflict.类型}</td>
                  <td>{conflict.算子编号 ?? '—'}</td>
                  <td>{conflict.批次号}</td>
                  <td>{(conflict.涉及存档 ?? []).join('、')}</td>
                  <td>{conflict.说明}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
      )}

      <footer className="page-foot">
        <span>共 {total} 条算子执行数据存档，每条均保留批次号与生效并行度可核对</span>
        {message && <span className="stat-label">{message}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
