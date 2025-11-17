import { Link } from "react-router-dom"
import wind from '../assets/wind-turbines-farmland.jpg'

export default function Home(){
    return(
        <>
         <div
                className="min-h-screen flex items-center justify-center p-4"
                style={{
                    backgroundImage: `url(${wind})`,
                    backgroundSize: 'cover',
                    backgroundPosition: 'center',
                }}
            >
                 <div className=" min-h-screen flex items-center justify-center p-4 space-y-4">
                <div className="text-center ">
                    <h1 className="font-bold text-9xl  text-yellow-300 font-sans">Welcome to Energy Forecast</h1>
                    <div className="space-y-4">
                        <p className="gap-2 text-bold  ">Includes realtime weather analysis with our proprietary AI model to give an accurate energy forecast</p>
                    </div>
                    <div className="space-y-4">
                        <button className=" bg-[#20a7db] opacity-50 w-[20%] h-[10%] text-white hover:text-yellow-300 rounded-xl p-4">
                            <Link to='/pre'>Get Started</Link>
                        </button>
                    </div>
                    
                   
                </div>
            </div>
            </div>
           
        </>
    )
}