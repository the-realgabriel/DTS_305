import Home from './pages/home'
import './App.css'
import { BrowserRouter as Router, Route, Routes} from 'react-router-dom'

function App() {
  

  return (
    <>
    <Router>
      <Routes>
        <Route path='/' element={<Home/>}/>
      </Routes>
    </Router>

    </>
  )
}

export default App
