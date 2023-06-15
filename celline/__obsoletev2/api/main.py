from flask import Flask, request
from flask_cors import CORS
import sys
from celline.database import NCBI, GSE, GSM, SRR
from celline.config import Config, Setting
from celline.data.files import FileManager
from celline.functions.dump import Dump
from celline.plugins.collections.generic import DictionaryC
import json
from typing import Optional
import pandas as pd
from typing import List
from celline.server.connection import RemoteServer, ServerConnection


exec_path = sys.argv[1]
proj_path = sys.argv[2]

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

Config.initialize(exec_path, proj_path, cmd="interactive")
Setting.initialize()


@app.route('/', methods=["GET"])
def index():
    return "Hello TEST"


@app.route("/addgsm", methods=["GET"])
def addgsm():
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            NCBI.add(id)
            return "SUCESS"
    finally:
        print("SUCESS")


@app.route("/addgse", methods=["GET"])
def addgse():
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            NCBI.add(id, verbose=True, interactive=False)
            return "SUCESS"
    finally:
        print("SUCESS")


@app.route("/postgsm", methods=["POST"])
def postgsm():
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            gse = GSE.search(id, return_error=True)
            if isinstance(gse, str):
                return id
            runs = FileManager.read_runs()
            runs = pd.concat(
                [
                    pd.DataFrame(runs[~(runs["gsm_id"].isin([
                        d["accession"] for d in gse.child_gsm_ids
                    ]))]).reset_index(),
                    pd.DataFrame(
                        data={
                            "gsm_id": json.loads(request.data)["gsm_id"],
                            "sample_name": json.loads(request.data)["sample_name"]
                        }
                    ).reset_index()
                ]
            ).set_index("gsm_id")
            del runs["index"]
            runs.to_csv(
                f"{Config.PROJ_ROOT}/runs.tsv",
                sep="\t"
            )
            return "SUCESS"
    finally:
        print("SUCESS")


@app.route("/getgse", methods=["GET"])
def get_gse():
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            result = GSE.search(id, return_error=True)
            if isinstance(result, str):
                return result
            else:
                return json.dumps({
                    "runid": result.runid,
                    "title": result.title,
                    "link": f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={result.runid}",
                    "summary": result.summary,
                    "species": result.species,
                    "raw_link": result.raw_link,
                    "gsms": result.child_gsm_ids,
                    "target_gsms": [id for id in result.child_gsm_ids if id["accession"] in FileManager.read_runs()["gsm_id"].to_list()]
                })
    finally:
        print("SUCESS")


@app.route("/getgsm", methods=["GET"])
def get_child_gsm_in_gse():
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            all_data = NCBI.get_gsms()
            target_gsmid = []
            for gsm in all_data:
                if all_data[gsm].parent_gse_id == id:
                    target_gsmid.append({
                        "gsm_id": gsm
                    })
            return target_gsmid
    finally:
        print("SUCESS")


@app.route("/gses", methods=["GET"])
def get_project_gses():
    # with open(f"{Config.EXEC_ROOT}/test.log", mode="w") as f:
    #     f.writelines(FileManager.read_runs()["gsm_id"].to_list())
    return json.dumps([{
        "runid": d.runid,
        "title": d.title,
        "link": f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={d.runid}",
        "summary": d.summary,
        "species": d.species,
        "raw_link": d.raw_link,
        "gsms": d.child_gsm_ids,
        "target_gsms": [id for id in d.child_gsm_ids if id["accession"] in FileManager.read_runs()["gsm_id"].to_list()]
    } for d in GSE.runs])


@app.route("/srr", methods=["GET"])
def srr():
    """Input: GSM ID"""
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            srrs: List[SRR] = []
            for srr_id in GSM.search(id).child_srr_ids:
                srrs.append(SRR.search(srr_id, ignoreerror=True))
            return json.dumps(
                [
                    {
                        "filetype": d.file_type.name,
                        "run_id": d.runid,
                        "sc_runs": [
                            {
                                "cloud_path": run.cloud_path.path,
                                "filesize": run.filesize.sizeGB,
                                "lane": run.lane.name,
                                "readtype": run.readtype.name,
                            } for run in d.sc_runs
                        ]
                    } for d in srrs
                ]
            )
    finally:
        print("SUCESS")


@app.route("/dump", methods=["GET"])
def dump():
    """Dump target data in GSE project"""
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "INVALID ID"
        else:
            target_gse = GSE.search(id, return_error=True)
            if isinstance(target_gse, str):
                return target_gse
            options = DictionaryC[str, Optional[str]]()
            options.Add("gsm", ",".join([d["accession"]
                        for d in target_gse.child_gsm_ids]))
            options.Add("nthread", "10")
            options.Add("job", "PBS@yuri")
            options.Add("norun", "")
            Dump().on_call(
                args={
                    "options": options
                }
            )
            return "SUCESS"
    finally:
        print("SUCESS")


@app.route("/status", methods=["GET"])
def status():
    return "Constructing"


@app.route("/servers", methods=["GET", "POST", "DELETE"])
def servers():
    try:
        req = request.args
        id = req.get("id")
        if request.method == "GET":
            if id is None:
                # Get all
                servers = RemoteServer.servers()
                servers_compiled: List = []
                for name in servers:
                    servers_compiled.append(vars(servers[name]))
                return json.dumps(servers_compiled)
            else:
                if RemoteServer.contains(id):
                    return json.dumps(vars(RemoteServer(id).target_server))
                else:
                    return f"Could not find target server: {id}"
        elif request.method == "POST":
            if id is None:
                return "Multiple update is not allowed."
            else:
                server = RemoteServer.ServerSettingStruct(
                    **json.loads(request.data))
                if id != server.name:
                    return "Given server name and id incoinsident"
                else:
                    RemoteServer.add_or_update_server(
                        server.name,
                        server.ip,
                        server.uname,
                        server.secretkey_path,
                        server.port
                    )
                    return f"SUCESS"
        elif request.method == "DELETE":
            if id is None:
                return "Multiple update is not allowed."
            else:
                if RemoteServer.delete(id):
                    return "SUCESS"
                else:
                    return "Could not delete target server"
        else:
            return "Unknown request"
    finally:
        print("SUCESS")


@app.route("/server_status", methods=["GET"])
def server_status():
    try:
        req = request.args
        id = req.get("id")
        if id is None:
            return "ID_UNKNOWN"
        if not RemoteServer.contains(id):
            return f"Unknown server: {id}"
        server = RemoteServer(id)
        try:
            with ServerConnection(server) as connection:
                _, stdout, stderr = connection.execute("echo Hello world")
                if str(stdout) == "Hello world":
                    return str(True)
                else:
                    return str(False)
        except TimeoutError:
            return str(False)
    finally:
        print("SUCESS")


app.run(port=8000, debug=True)
