import React from 'react';
import Body from '../components/Body';
import Posts from '../components/Posts';

export default function FeedPage() {
  return (
    <Body sidebar>
      <Posts />
    </Body>
  );
}