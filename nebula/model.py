
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import sqlalchemy
from sqlalchemy import or_, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased, relationship


Base = declarative_base()


class FileMapping(Base):
    __tablename__ = 'file_mapping'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    local_path = sqlalchemy.Column(sqlalchemy.Text, unique=True, index=True)
    md5 = sqlalchemy.Column(sqlalchemy.String(32))
    galaxy_id = sqlalchemy.Column(sqlalchemy.String(30))
    file_type = sqlalchemy.Column(sqlalchemy.String(32))

    def __init__(self, file_type, path, md5, gid):
        self.file_type = file_type
        self.local_path = path
        self.md5 = md5
        self.galaxy_id = gid

class WorkflowRun(Base):
    __tablename__ = 'workflow_run'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    history_galaxy_id = sqlalchemy.Column(sqlalchemy.String(30))
    workflow_id = sqlalchemy.Column(sqlalchemy.Integer)

    def __init__(self, workflow_id, history_galaxy_id):
        self.workflow_id = workflow_id
        self.history_galaxy_id = history_galaxy_id



class SQLiteMapping(alfredo.FileGalaxyMap):

    def __init__(self, sess):
        alfredo.FileGalaxyMap.__init__(self, sess)
        url = "sqlite:///" + os.path.join( os.path.abspath(self.sess.basedir), "local.mapping")
        self.engine = sqlalchemy.create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)
        self.sql_sessionmaker = sessionmaker(bind=self.engine)
        self.sql_sess = self.sql_sessionmaker()

    def _find_local_file(self, file_type, path):
        relpath = os.path.relpath(path, self.sess.basedir)
        hmd5 = self._calc_md5(path)
        ref = self.sql_sess.query( FileMapping ).filter( FileMapping.file_type == file_type, FileMapping.md5 == hmd5, FileMapping.local_path == relpath ).first()
        if ref is not None:
            return ref.galaxy_id
        return None

    def _add_local_file(self, file_type, path, gid):
        relpath = os.path.relpath(path, self.sess.basedir)
        hmd5 = self._calc_md5(relpath)
        wm = FileMapping(file_type, relpath, hmd5, gid)
        self.sql_sess.add(wm)
        self.sql_sess.commit()

    def find_workflow_run(self, workflow_gid, id_mapping):
        q = self.sql_sess.query( WorkflowRun )
        for i in id_mapping:
            a = aliased( WorkflowFileLink )
            f = aliased( FileMapping )
            q = q.filter(
                and_(
                    a.run_id == WorkflowRun.id,
                    a.input_number == i,
                    a.file_id == f.id,
                    f.galaxy_id == id_mapping[i]['id']
                )
            )
            res = q.first()
            if res is not None:
                return res.history_galaxy_id
            return None

    def add_workflow_run(self, workflow_gid, id_mapping, history_id):
        w_res = self.sql_sess.query(FileMapping).filter( FileMapping.galaxy_id == workflow_gid ).first()
        if w_res is None:
            raise alfredo.ElementNotFound(workflow_gid)

        wfr = WorkflowRun(w_res.id, history_id)
        self.sql_sess.add(wfr)
        self.sql_sess.commit()

        for i in id_mapping:

            d_res = self.sql_sess.query( FileMapping ).filter( FileMapping.galaxy_id == id_mapping[i]['id'] ).first()
            if d_res is None:
                raise alfredo.ElementNotFound( id_mapping[i]['id'] )
            wfl = WorkflowFileLink( wfr.id, i, d_res.id )
            self.sql_sess.add(wfl)
            self.sql_sess.commit()

    def find_local_workflow(self, path):
        return self._find_local_file("workflow", path)

    def add_local_workflow(self, path, gid):
        self._add_local_file("workflow", path, gid)

    def find_local_dataset(self, path):
        return self._find_local_file("dataset", path)

    def add_local_dataset(self, path, gid):
        self._add_local_file("dataset", path, gid)

    def _calc_md5(self,path):
        md5 = hashlib.md5()
        f = open(path,'rb')
        smeta = ""
        block_size = 80000
        while True:
            data = f.read(block_size)
            if not data:
                break
            md5.update(data)
            smeta += data
        return md5.hexdigest()
