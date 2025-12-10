import { useState, useEffect } from 'react'

type WeatherInfo = {
    temp?: number
    wind_speed?: number
    humidity?: number
    description?: string
    raw?: any
}

export default function Predict(){
    const [historicalData, setHistoricalData] = useState('')
    const [prediction, setPrediction] = useState<number | null>(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)

    // weather-related state
    const [autoLocation, setAutoLocation] = useState(true)
    const [weather, setWeather] = useState<WeatherInfo | null>(null)
    const [fetchingWeather, setFetchingWeather] = useState(false)
    const [location, setLocation] = useState<string>('')
    const [locationPermission, setLocationPermission] = useState<'prompt' | 'granted' | 'denied'>('prompt')

    // Additional fields for complete data points
    const [voltage, setVoltage] = useState('230')
    const [current, setCurrent] = useState('10')

    const SEQUENCE_LENGTH = 2

    async function fetchWeatherByCoords(lat: number, lon: number) {
        try {
            // Using a free weather API that doesn't require API key
            const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&temperature_unit=celsius&wind_speed_unit=ms`
            const res = await fetch(url)
            if (!res.ok) throw new Error(`Weather API error ${res.status}`)
            const data = await res.json()
            
            // Map weather codes to descriptions
            const weatherDescriptions: Record<number, string> = {
                0: 'Clear sky',
                1: 'Mainly clear',
                2: 'Partly cloudy',
                3: 'Overcast',
                45: 'Foggy',
                48: 'Depositing rime fog',
                51: 'Light drizzle',
                53: 'Moderate drizzle',
                55: 'Dense drizzle',
                61: 'Slight rain',
                63: 'Moderate rain',
                65: 'Heavy rain',
                71: 'Slight snow',
                73: 'Moderate snow',
                75: 'Heavy snow',
                77: 'Snow grains',
                80: 'Slight rain showers',
                81: 'Moderate rain showers',
                82: 'Violent rain showers',
                85: 'Slight snow showers',
                86: 'Heavy snow showers',
                95: 'Thunderstorm',
                96: 'Thunderstorm with slight hail',
                99: 'Thunderstorm with heavy hail'
            }
            
            const w: WeatherInfo = {
                temp: data?.current?.temperature_2m,
                humidity: data?.current?.relative_humidity_2m,
                wind_speed: data?.current?.wind_speed_10m,
                description: weatherDescriptions[data?.current?.weather_code] || 'Unknown',
                raw: data
            }
            
            // Use reverse geocoding to get location name
            try {
                const geoUrl = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lon}&format=json`
                const geoRes = await fetch(geoUrl, {
                    headers: { 'User-Agent': 'EnergyForecastApp/1.0' }
                })
                if (geoRes.ok) {
                    const geoData = await geoRes.json()
                    setLocation(geoData?.address?.city || geoData?.address?.town || geoData?.address?.village || 'Unknown Location')
                }
            } catch {
                setLocation('Location Found')
            }
            
            return w
        } catch (err: any) {
            setError(err?.message || 'Failed to fetch weather')
            return null
        }
    }

    async function getLocationAndWeather() {
        setError(null)
        setFetchingWeather(true)

        if (!navigator.geolocation) {
            setError('Geolocation not supported by this browser')
            setFetchingWeather(false)
            return
        }

        navigator.geolocation.getCurrentPosition(
            async (pos) => {
                const lat = pos.coords.latitude
                const lon = pos.coords.longitude
                const w = await fetchWeatherByCoords(lat, lon)
                if (w) {
                    setWeather(w)
                    setLocationPermission('granted')
                }
                setFetchingWeather(false)
            },
            (err) => {
                if (err.code === err.PERMISSION_DENIED) {
                    setLocationPermission('denied')
                    setError('Location access denied. Please enable location permissions in your browser settings.')
                } else if (err.code === err.POSITION_UNAVAILABLE) {
                    setError('Location information unavailable. Please check your device settings.')
                } else if (err.code === err.TIMEOUT) {
                    setError('Location request timed out. Please try again.')
                } else {
                    setError(`Geolocation error: ${err.message}`)
                }
                setFetchingWeather(false)
            },
            { enableHighAccuracy: true, timeout: 15000, maximumAge: 300000 }
        )
    }

    // Auto-fetch weather on component mount
    useEffect(() => {
        const checkPermissionAndFetch = async () => {
            if (navigator.permissions && navigator.geolocation) {
                try {
                    const result = await navigator.permissions.query({ name: 'geolocation' as PermissionName })
                    setLocationPermission(result.state as 'prompt' | 'granted' | 'denied')
                    
                    if (result.state === 'granted') {
                        getLocationAndWeather()
                    }
                    
                    // Listen for permission changes
                    result.addEventListener('change', () => {
                        setLocationPermission(result.state as 'prompt' | 'granted' | 'denied')
                        if (result.state === 'granted') {
                            getLocationAndWeather()
                        }
                    })
                } catch {
                    // Permissions API not supported, try direct geolocation
                    if (autoLocation) {
                        getLocationAndWeather()
                    }
                }
            }
        }
        
        checkPermissionAndFetch()
    }, [])

    async function handleSubmit() {
        setError(null)
        setPrediction(null)

        // Parse power consumption values
        let powerValues = historicalData
            .split(',')
            .map(s => parseFloat(s.trim()))
            .filter(n => !Number.isNaN(n))

        if (powerValues.length < SEQUENCE_LENGTH) {
            setError(`Please provide at least ${SEQUENCE_LENGTH} power consumption values (comma-separated).`)
            return
        }

        // Limit to exactly SEQUENCE_LENGTH most recent values
        powerValues = powerValues.slice(-SEQUENCE_LENGTH)

        setLoading(true)
        try {
            // if autoLocation requested but no weather yet, try to fetch
            if (autoLocation && !weather) {
                await getLocationAndWeather()
            }

            // Build the data array with all required fields
            const now = new Date()
            const dataPoints = powerValues.map((power, idx) => {
                // Create timestamps going backwards from now
                const timestamp = new Date(now.getTime() - (SEQUENCE_LENGTH - idx - 1) * 3600000)
                
                return {
                    timestamp: timestamp.toISOString(),
                    Power_Consumption: power,
                    voltage: parseFloat(voltage) || 230,
                    current: parseFloat(current) || 10,
                    temperature: weather?.temp || 20,
                    humidity: weather?.humidity || 50
                }
            })

            const res = await fetch('http://127.0.0.1:8000/predict', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(dataPoints),
            })

            let data: any = null
            try { data = await res.json() } catch {}

            if (!res.ok) {
                const msg = data?.error || `Request failed (status ${res.status})`
                setError(msg)
            } else {
                const pred = data?.predicted_energy_kwh
                if (typeof pred === 'number' || !Number.isNaN(Number(pred))) {
                    setPrediction(Number(pred))
                } else {
                    setError('Backend returned unexpected response')
                }
            }
        } catch (err: any) {
            setError(err?.message || 'Network error')
        } finally {
            setLoading(false)
        }
    }

    return(
        <div className="min-h-screen bg-gradient-to-br from-blue-400 via-blue-500 to-purple-600 p-6">
            <div className="max-w-6xl mx-auto">
                {/* Header */}
                <div className="text-center mb-8">
                    <h1 className="text-5xl font-bold text-white mb-2">Energy Forecast Dashboard</h1>
                    <p className="text-blue-100">Hybrid AI-powered energy consumption predictions (LSTM + Random Forest + GBT)</p>
                </div>

                {/* Main Grid */}
                <div className="grid md:grid-cols-2 gap-6 mb-6">
                    {/* Weather Card */}
                    <div className="bg-white/10 backdrop-blur-md rounded-3xl p-8 text-white shadow-xl border border-white/20">
                        <h2 className="text-2xl font-bold mb-6 flex items-center gap-2">
                             Current Weather
                        </h2>

                        {weather ? (
                            <div className="space-y-4">
                                {location && (
                                    <div className="flex items-center justify-between">
                                        <p className="text-xl font-semibold">{location}</p>
                                        <button
                                            onClick={getLocationAndWeather}
                                            disabled={fetchingWeather}
                                            className="text-xs bg-white/20 hover:bg-white/30 px-3 py-1 rounded-lg transition-all disabled:opacity-50"
                                        >
                                            {fetchingWeather ? '🔄' : '↻ Refresh'}
                                        </button>
                                    </div>
                                )}
                                
                                <div className="grid grid-cols-3 gap-4">
                                    <div className="bg-white/20 rounded-2xl p-4 text-center">
                                        <p className="text-sm opacity-80 mb-1">Temperature</p>
                                        <p className="text-3xl font-bold">{weather.temp?.toFixed(1) ?? '—'}°C</p>
                                    </div>
                                    <div className="bg-white/20 rounded-2xl p-4 text-center">
                                        <p className="text-sm opacity-80 mb-1">Humidity</p>
                                        <p className="text-3xl font-bold">{weather.humidity ?? '—'}%</p>
                                    </div>
                                    <div className="bg-white/20 rounded-2xl p-4 text-center">
                                        <p className="text-sm opacity-80 mb-1">Wind Speed</p>
                                        <p className="text-3xl font-bold">{weather.wind_speed?.toFixed(1) ?? '—'} m/s</p>
                                    </div>
                                </div>

                                <div className="bg-white/20 rounded-2xl p-4">
                                    <p className="text-sm opacity-80 mb-1">Conditions</p>
                                    <p className="text-lg capitalize">{weather.description ?? 'N/A'}</p>
                                </div>
                            </div>
                        ) : (
                            <div className="text-center py-8">
                                <p className="text-blue-100 mb-4">
                                    {locationPermission === 'denied' 
                                        ? '⚠️ Location access denied'
                                        : locationPermission === 'granted'
                                        ? 'Loading weather data...'
                                        : 'Enable location to get weather data'}
                                </p>
                                <button
                                    onClick={getLocationAndWeather}
                                    disabled={fetchingWeather}
                                    className="bg-white/30 hover:bg-white/40 text-white font-semibold py-2 px-6 rounded-xl transition-all disabled:opacity-50"
                                >
                                    {fetchingWeather ? '📍 Locating...' : locationPermission === 'denied' ? '🔓 Enable Location' : '📍 Fetch Weather'}
                                </button>
                                {locationPermission === 'denied' && (
                                    <p className="text-xs text-blue-100 mt-3">
                                        Check your browser's location settings
                                    </p>
                                )}
                            </div>
                        )}
                    </div>

                    {/* Prediction Card */}
                    <div className="bg-white/10 backdrop-blur-md rounded-3xl p-8 text-white shadow-xl border border-white/20">
                        <h2 className="text-2xl font-bold mb-6 flex items-center gap-2">
                            <span>⚡</span> Energy Prediction
                        </h2>

                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-semibold mb-2">
                                    Historical Power Consumption (kWh) - {SEQUENCE_LENGTH} values required
                                </label>
                                <textarea
                                    className="w-full bg-white/20 border border-white/30 rounded-xl px-4 py-3 text-white placeholder-white/50 focus:outline-none focus:ring-2 focus:ring-white/50"
                                    value={historicalData}
                                    onChange={e => setHistoricalData(e.target.value)}
                                    placeholder="e.g. 10.5, 11.2, 9.8, 12.0, ... (24 values)"
                                    rows={3}
                                />
                                <p className="text-xs text-blue-100 mt-1">
                                    Comma-separated values (minimum {SEQUENCE_LENGTH} points)
                                </p>
                            </div>

                            {/* Additional Parameters */}
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-xs font-semibold mb-1">Voltage (V)</label>
                                    <input
                                        type="number"
                                        className="w-full bg-white/20 border border-white/30 rounded-xl px-3 py-2 text-white placeholder-white/50 focus:outline-none focus:ring-2 focus:ring-white/50"
                                        value={voltage}
                                        onChange={e => setVoltage(e.target.value)}
                                        placeholder="230"
                                    />
                                </div>
                                <div>
                                    <label className="block text-xs font-semibold mb-1">Current (A)</label>
                                    <input
                                        type="number"
                                        className="w-full bg-white/20 border border-white/30 rounded-xl px-3 py-2 text-white placeholder-white/50 focus:outline-none focus:ring-2 focus:ring-white/50"
                                        value={current}
                                        onChange={e => setCurrent(e.target.value)}
                                        placeholder="10"
                                    />
                                </div>
                            </div>

                            {/* Options */}
                            <div className="space-y-2 bg-white/10 rounded-xl p-4">
                                <label className="flex items-center gap-2 cursor-pointer">
                                    <input 
                                        type="checkbox" 
                                        checked={autoLocation} 
                                        onChange={() => setAutoLocation(v => !v)} 
                                        className="w-4 h-4" 
                                    />
                                    <span className="text-sm">Use real-time weather data for prediction</span>
                                </label>
                            </div>

                            {/* Buttons */}
                            <div className="flex gap-3 pt-2">
                                <button
                                    onClick={handleSubmit}
                                    className="flex-1 bg-white text-blue-600 font-bold py-3 rounded-xl hover:bg-blue-50 transition-all disabled:opacity-50"
                                    disabled={loading}
                                >
                                    {loading ? '⏳ Predicting...' : '🔮 Predict Next Hour'}
                                </button>
                                <button
                                    onClick={() => { 
                                        setHistoricalData(''); 
                                        setPrediction(null); 
                                        setError(null);
                                        setVoltage('230');
                                        setCurrent('10');
                                    }}
                                    className="bg-white/30 hover:bg-white/40 text-white font-semibold py-3 px-6 rounded-xl transition-all"
                                >
                                    ↻ Reset
                                </button>
                            </div>
                        </div>

                        {/* Prediction Result */}
                        {prediction !== null && (
                            <div className="mt-6 bg-gradient-to-r from-green-400/20 to-blue-400/20 rounded-2xl p-6 border border-white/30">
                                <p className="text-sm opacity-80 mb-2">Predicted Energy Consumption</p>
                                <p className="text-5xl font-bold mb-1">{prediction.toFixed(3)}</p>
                                <p className="text-lg opacity-90">kWh</p>
                            </div>
                        )}
                    </div>
                </div>

                {/* Error Alert */}
                {error && (
                    <div className="bg-red-500/80 backdrop-blur-md rounded-2xl p-4 text-white border border-red-400/50 shadow-lg mb-6">
                        <p className="font-semibold">⚠️ Error</p>
                        <p className="text-sm mt-1">{error}</p>
                    </div>
                )}

                {/* Info Banner */}
                <div className="bg-white/10 backdrop-blur-md rounded-2xl p-6 text-white border border-white/20">
                    <h3 className="font-bold mb-3 flex items-center gap-2">
                        <span>ℹ️</span> How It Works
                    </h3>
                    <ul className="text-sm space-y-2 text-white/90">
                        <li>• Provide {SEQUENCE_LENGTH} historical power consumption readings (in kWh)</li>
                        <li>• Location access is automatically requested for real-time weather data</li>
                        <li>• Weather data (temperature & humidity) improves prediction accuracy</li>
                        <li>• The hybrid model combines LSTM, Random Forest, and Gradient Boosted Trees</li>
                        <li>• Get accurate predictions for the next hour's energy consumption</li>
                    </ul>
                    
                    {locationPermission === 'denied' && (
                        <div className="mt-4 p-3 bg-yellow-500/20 rounded-lg border border-yellow-400/30">
                            <p className="text-xs font-semibold mb-1">⚠️ Location Access Needed</p>
                            <p className="text-xs">To enable location: Click the 🔒 icon in your browser's address bar → Site settings → Location → Allow</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    )
}