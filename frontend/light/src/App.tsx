import Home from './pages/home'
import Predict from './pages/predict'
import './App.css'
import { BrowserRouter as Router, Route, Routes} from 'react-router-dom'

function App() {
  

  return (
    <>
    <Router>
      <Routes>
        <Route path='/' element={<Home/>}/>
        <Route path='/pre' element= {<Predict/>}/>
      </Routes>
    </Router>

    </>
  )
}

export default App
