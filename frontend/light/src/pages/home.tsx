import { Link } from "react-router-dom"
import wind from '../assets/wind-turbines-farmland.jpg'

export default function Home(){
    return(
        <>
         <div
                className="relative min-h-screen flex items-center justify-center p-4 bg-black"
                style={{
                    backgroundImage: `url(${wind})`,
                    backgroundSize: 'cover',
                    backgroundPosition: 'center',
                }}
            >
                {/* overlay for readability */}
                <div className="absolute inset-0 bg-black/50" />

                <div className="relative z-10 w-full max-w-4xl px-6 py-16 flex flex-col items-center text-center space-y-6">
                    <h1 className="font-bold text-yellow-300 font-sans leading-tight
                                   text-3xl sm:text-4xl md:text-5xl lg:text-6xl xl:text-7xl">
                        Welcome to Energy Forecast
                    </h1>

                    <p className="text-white/90 max-w-2xl text-sm sm:text-base md:text-lg">
                        Includes realtime weather analysis with our proprietary AI model to give an accurate energy forecast
                    </p>

                    <Link
                        to="/pre"
                        className="w-full sm:w-auto inline-flex items-center justify-center bg-[#20a7db] hover:bg-[#1b91bd] text-white font-medium rounded-lg px-6 py-3 transition-opacity duration-150"
                    >
                        Get Started
                    </Link>
                </div>
            </div>
        </>
    )
}