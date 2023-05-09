import React from 'react';
import { useState } from 'react';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import Body from '../components/Body';
import InputField from '../components/InputField';
//import { useApi } from '../contexts/ApiProvider';
import { useNavigate } from 'react-router-dom';
import { useEffect, useRef } from 'react';


export default function LoginPage() {
    // const [formErrors, setFormErrors] = useState({});
    // const usernameField = useRef();
    // const emailField = useRef();
    // const passwordField = useRef();
    // const navigate = useNavigate();
    // const api = useApi();

    const [hashtag, setHashtag] = useState('');
    const [response, setResponse] = useState(null);

    const handleSubmit = (event) => {
      event.preventDefault();
      console.log(hashtag)
        fetch("http://127.0.0.1:5000/api/endpoint", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Methods": "POST, PUT"
          },
          body: JSON.stringify(hashtag),
        })
        .then(response => response.json())
        .then(data => {
          console.log(data)
          setResponse(data)
          setHashtag('')
        })
        .catch(error => console.error(error))
    }
  
    const handleChange = (event) => {
      setHashtag(event.target.value);
    }

    return (
        <Form onSubmit={handleSubmit}>
          <Form.Group className="mb-3" controlId="formBasicText">
            <Form.Label>Hashtag</Form.Label>
            <Form.Control 
              type="textarea" 
              placeholder="Enter hashtag"
              value={hashtag}
              onChange={handleChange}
             />
          </Form.Group>
          <Button variant="primary" type="submit">
            Search tweets
          </Button>
        </Form> 
    );
}