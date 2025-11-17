import { useState } from 'react'

export default function Predict(){
    const [input, setInput] = useState('')
    const [prediction, setPrediction] = useState<number | null>(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)

    async function handleSubmit(e: React.FormEvent) {
        e.preventDefault()
        setError(null)
        setPrediction(null)

        const nums = input
            .split(',')
            .map(s => parseFloat(s.trim()))
            .filter(n => !Number.isNaN(n))

        if (nums.length < 3) {
            setError('Please provide at least 3 numeric values (comma-separated).')
            return
        }

        setLoading(true)
        try {
            const res = await fetch('http://127.0.0.1:5000/predict', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ history: nums }),
            })

            const data = await res.json()
            if (!res.ok) {
                setError(data.error || 'Request failed')
            } else {
                setPrediction(Number(data.prediction))
            }
        } catch (err: any) {
            setError(err?.message || 'Network error')
        } finally {
            setLoading(false)
        }
    }

    return(
        <>
        <div className="item-center justify-center p-4">
           
                <h1 className="text-center ">Please note for proper pridiction we'll need you to enable location tracking</h1>
          
        </div>

        <form onSubmit={handleSubmit} className="p-4 space-y-3">
            <label className="block">
                <span>Enter past values (comma-separated):</span>
                <input
                    className="border rounded px-2 py-1 mt-1 w-full"
                    value={input}
                    onChange={e => setInput(e.target.value)}
                    placeholder="e.g. 10.5, 11.2, 9.8, 12.0"
                />
            </label>

            <div className="flex items-center space-x-2">
                <button
                    type="submit"
                    className="bg-blue-500 text-white px-4 py-2 rounded"
                    disabled={loading}
                >
                    {loading ? 'Predicting...' : 'Predict'}
                </button>
                <button
                    type="button"
                    className="bg-gray-200 px-3 py-2 rounded"
                    onClick={() => { setInput(''); setPrediction(null); setError(null) }}
                >
                    Reset
                </button>
            </div>

            {prediction !== null && (
                <div className="mt-3">
                    <strong>Prediction:</strong> {prediction}
                </div>
            )}

            {error && <div className="text-red-600 mt-2">{error}</div>}
        </form>
        </>
    )
}