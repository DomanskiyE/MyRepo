from typing import Dict, Any, Union
from uuid import uuid4
import os
import subprocess
from flask import Flask, request, jsonify, send_file
from flask_restful import Resource, Api
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, validator


app = Flask(__name__)
api = Api(app)

engine = create_engine('postgresql://postgres:example@db/app_db')
Session = sessionmaker(bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(String, primary_key=True)
    name = Column(String)
    token = Column(String)

    recordings = relationship('Recording', back_populates='user')


class Recording(Base):
    __tablename__ = 'recordings'

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey('users.id'))
    url = Column(String)

    user = relationship('User', back_populates='recordings')


Base.metadata.create_all(engine)


class UserCreateRequest(BaseModel):
    name: str

    @validator('name')
    def name_must_not_be_blank(cls, v):
        if not v.strip():
            raise ValueError('name must not be blank')
        return v


class RecordingUploadRequest(BaseModel):
    user_id: str
    token: str
    audio_file: bytes

    @validator('user_id')
    def user_id_must_not_be_blank(cls, v):
        if not v.strip():
            raise ValueError('user_id must not be blank')
        return v

    @validator('token')
    def token_must_not_be_blank(cls, v):
        if not v.strip():
            raise ValueError('token must not be blank')
        return v


class UserCreateResource(Resource):
    def post(self):
        session = Session()

        request_data = UserCreateRequest(**request.json)

        user_id = str(uuid4())
        token = str(uuid4())

        user = User(id=user_id, name=request_data.name, token=token)
        session.add(user)
        session.commit()

        return {'user_id': user_id, 'token': token}


class RecordingUploadResource(Resource):
    def post(self):
        session = Session()

        request_data = RecordingUploadRequest(**request.form)

        user = session.query(User).filter_by(id=request_data.user_id, token=request_data.token).first()
        if not user:
            return {'error': 'invalid user_id or token'}, 401

        audio_file = request.files['audio_file']
        if not audio_file:
            return {'error': 'audio_file is required'}, 400

        audio_id = str(uuid4())
        audio_dir = os.path.join(os.getcwd(), 'audio')
        if not os.path.exists(audio_dir):
            os.makedirs(audio_dir)

        wav_path = os.path.join(audio_dir, f'{audio_id}.wav')
        audio_file.save(wav_path)

        mp3_path = os.path.join(audio_dir, f'{audio_id}.mp3')
        subprocess.run(['ffmpeg', '-i', wav_path, '-codec:a', 'libmp3lame', '-qscale:a', '2', mp3_path], check=True)

        recording = Recording(id=audio_id, user=user, url=f'http://localhost:8000/record?id={audio_id}&user={user.id}')
        session.add(recording)
        session.commit()

        return {'url': recording.url}


class RecordingDownloadResource(Resource):
    def get(self):
        recording_id = request.args.get('id')
        user_id = request.args.get('user')

        session = Session()
        recording = session.query(Recording).filter_by(id=recording_id, user_id=user_id).first()
        if not recording:
            return {'error': 'recording not found'}, 404

        audio_path = os.path.join(os.getcwd(), 'audio', f'{recording_id}.mp3')
        return send_file(audio_path)


api.add_resource(UserCreateResource, '/user')
api.add_resource(RecordingUploadResource, '/recording')
api.add_resource(RecordingDownloadResource, '/record')


if __name__ == '__main__':
    app.run()