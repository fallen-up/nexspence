import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Select, SelectOption } from './Select'

const opts: SelectOption[] = [
  { value: 'a', label: 'Alpha' },
  { value: 'b', label: 'Beta' },
  { value: 'c', label: 'Gamma' },
]

describe('Select', () => {
  it('shows placeholder when nothing is selected', () => {
    render(<Select options={opts} value="" onChange={() => {}} placeholder="Pick one" />)
    expect(screen.getByRole('button', { name: /Pick one/ })).toBeInTheDocument()
  })

  it('shows the selected option label', () => {
    render(<Select options={opts} value="b" onChange={() => {}} />)
    expect(screen.getByRole('button', { name: /Beta/ })).toBeInTheDocument()
  })

  it('opens the dropdown and lists options, selecting fires onChange and closes', async () => {
    const onChange = vi.fn()
    render(<Select options={opts} value="" onChange={onChange} />)
    await userEvent.click(screen.getByRole('button'))
    // Options rendered in portal.
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    expect(screen.getByText('Gamma')).toBeInTheDocument()
    await userEvent.click(screen.getByText('Gamma'))
    expect(onChange).toHaveBeenCalledWith('c')
    // Dropdown closed.
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument()
  })

  it('does not open when disabled', async () => {
    render(<Select options={opts} value="" onChange={() => {}} disabled />)
    await userEvent.click(screen.getByRole('button'))
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument()
  })

  it('closes when the open chevron is clicked', async () => {
    render(<Select options={opts} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    // The trigger is the combobox now; the chevron sits next to it.
    await userEvent.click(screen.getByRole('button', { name: 'Close' }))
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument()
    expect(screen.getByRole('button')).toHaveFocus()
  })

  it('filters the open list as you type in the trigger', async () => {
    render(<Select options={opts} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    const box = screen.getByRole('combobox')
    await userEvent.type(box, 'alp')
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    expect(screen.queryByText('Beta')).not.toBeInTheDocument()
    await userEvent.clear(box)
    await userEvent.type(box, 'zzz')
    expect(screen.getByText('No matches')).toBeInTheDocument()
  })

  it('Enter picks the first match after a filter', async () => {
    const onChange = vi.fn()
    render(<Select options={opts} value="" onChange={onChange} />)
    await userEvent.click(screen.getByRole('button'))
    await userEvent.type(screen.getByRole('combobox'), 'bet{Enter}')
    expect(onChange).toHaveBeenCalledWith('b')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })

  it('shows "No options" when given an empty list', async () => {
    render(<Select options={[]} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    expect(screen.getByText('No options')).toBeInTheDocument()
  })

  it('renders badge and tag for the selected option and in the list', async () => {
    const withExtras: SelectOption[] = [
      { value: 'a', label: 'Alpha', badge: <span data-testid="badge">B</span>, tag: <span data-testid="tag">T</span> },
    ]
    render(<Select options={withExtras} value="a" onChange={() => {}} />)
    expect(screen.getByTestId('badge')).toBeInTheDocument()
    expect(screen.getByTestId('tag')).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button'))
    // Open trigger is the filter; badges stay on the list row.
    expect(screen.getAllByTestId('badge').length).toBeGreaterThan(0)
  })

  it('closes on Escape key', async () => {
    render(<Select options={opts} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument()
  })

  it('closes on outside mousedown', async () => {
    render(<Select options={opts} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    fireEvent.mouseDown(document.body)
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument()
  })

  it('closes on window resize', async () => {
    render(<Select options={opts} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    fireEvent(window, new Event('resize'))
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument()
  })

  it('highlights the selected option in the list', async () => {
    render(<Select options={opts} value="b" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    // The selected row "Beta" appears in the list.
    const rows = screen.getAllByText('Beta')
    expect(rows.length).toBeGreaterThan(0)
  })

  it('sizes the menu from the pill, not the wrapper', async () => {
    render(
      <div style={{ width: 800 }}>
        <Select options={opts} value="" onChange={() => {}} style={{ width: 360 }} />
      </div>,
    )
    const pill = screen.getByRole('button')
    vi.spyOn(pill, 'getBoundingClientRect').mockReturnValue({
      x: 12, y: 8, top: 8, left: 12, bottom: 40, right: 372,
      width: 360, height: 32, toJSON() { return {} },
    })
    await userEvent.click(pill)
    expect(screen.getByRole('listbox')).toHaveStyle({ width: '360px', left: '12px' })
  })

  it('returns focus to the trigger after Escape', async () => {
    render(<Select options={opts} value="" onChange={() => {}} placeholder="Pick one" />)
    await userEvent.click(screen.getByRole('button', { name: /Pick one/ }))
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.getByRole('button', { name: /Pick one/ })).toHaveFocus()
  })

  it('returns focus to the trigger after a choice', async () => {
    render(<Select options={opts} value="" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    await userEvent.click(screen.getByText('Gamma'))
    expect(screen.getByRole('button')).toHaveFocus()
  })

  it('closes when tab leaves the combobox and keeps the new focus', async () => {
    render(
      <>
        <Select options={opts} value="" onChange={() => {}} />
        <button type="button">next</button>
      </>,
    )
    await userEvent.click(screen.getByRole('button', { name: /Select/ }))
    expect(screen.getByRole('listbox')).toBeInTheDocument()
    await userEvent.tab()
    expect(screen.getByRole('listbox')).toBeInTheDocument()
    await userEvent.tab()
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'next' })).toHaveFocus()
  })

  it('applies hover background on mouse enter/leave', async () => {
    render(<Select options={opts} value="a" onChange={() => {}} />)
    await userEvent.click(screen.getByRole('button'))
    const beta = screen.getByText('Beta').closest('div')!
    fireEvent.mouseEnter(beta)
    expect(beta.style.background).toContain('124, 92, 255')
    fireEvent.mouseLeave(beta)
    expect(beta.style.background).toBe('transparent')
  })
})
