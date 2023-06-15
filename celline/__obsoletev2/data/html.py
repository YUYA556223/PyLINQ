from __future__ import annotations  # type: ignore
from celline.plugins.collections.generic import ListC
from requests_html import HTMLResponse
import re
from typing import List
import pandas as pd


class HTMLStructure:
    cloud_path: str
    filetype: str
    filesize: str
    spieces: str
    gsmid: str
    srrid: str
    read: str

    def __init__(
        self,
        filesize: str,
        cloud_path: str,
        filetype: str,
        spieces: str,
        gsmid: str,
        srrid: str,
    ) -> None:
        self.cloud_path = cloud_path
        self.filetype = filetype
        self.filesize = filesize
        self.spieces = spieces
        self.gsmid = gsmid
        self.srrid = srrid
        pass

    @staticmethod
    def build(response: HTMLResponse) -> ListC[HTMLStructure]:
        # Count location information (e.g AWS, GCP)
        locnum = 0
        while True:
            __data = response.html.find(
                f"#ph-run-browser-data-access > div:nth-child(1) > table > tbody > tr:nth-child({locnum + 1})"
            )
            if len(__data) == 0:  # type:ignore
                break
            locnum += 1
        if locnum == 0:
            print("[ERROR] Could not find target data.")
            quit()
        # Get all metadata
        num = 0
        metainfo = response.html.find("#ph-run_browser > h1")[0].text.split(  # type: ignore
            ";"
        )
        gsmid = metainfo[0].split(":")[0]
        spieces = metainfo[1].replace(" ", "")
        srrid = metainfo[2].split("(")[1].replace(")", "")
        raw_data = response.html.find(
            "#ph-run-browser-data-access > div:nth-child(2) > table > tbody"
        )[
            0
        ]  # type: ignore
        __header = response.html.find(
            "#ph-run-browser-data-access > div:nth-child(2) > table > thead > tr"
        )[0].text.split(
            "\n"
        )  # type: ignore
        __append_target: List[str] = []
        for el in list(raw_data.find("tr")):  # type: ignore
            el_text = el.text.split("\n")
            if len(el_text) == len(__header):
                __append_target.append(el_text)
        data_df = pd.DataFrame(data=__append_target, columns=__header)
        results: ListC[HTMLStructure] = ListC[HTMLStructure]()
        for _, data in data_df.iterrows():
            ftype = "Unknown"
            if ".fastq" in data["Name"] or ".fq" in data["Name"]:
                ftype = "fastq"
            elif ".bam" in data["Name"]:
                ftype = "bam"
            results.Add(
                HTMLStructure(
                    filesize=data["Size"],  # type: ignore
                    cloud_path=data["Name"],  # type: ignore
                    filetype=ftype,
                    spieces=spieces,
                    gsmid=gsmid,
                    srrid=srrid,
                )
            )

        if results.Length == 0:
            print("[ERROR] Could not find runs")
            quit()
        if results.First().filetype == "fastq":
            if results.Length == 1:
                print("[ERROR] Only one read exists. Only paired reads are supported.")
                quit()
            elif results.Length == 2:
                num = 1
                for result in results:
                    result.read = f"R{num}"
                    num += 1
                del num
            elif (results.Length == 3) or (results.Length == 4):
                for result in results:
                    clname = result.cloud_path.split("/")[-1]
                    search_result_R = re.search("_R", clname)
                    if search_result_R is not None:
                        index = search_result_R.span()[1]
                        result.read = f"R{clname[index]}"
                        del search_result_R
                        del index
                    else:
                        search_result_I = re.search("_I", clname)
                        if search_result_I is not None:
                            index = search_result_I.span()[1]
                            result.read = f"I{clname[index]}"
                            del search_result_R
                            del index
                        else:
                            print(
                                "[Warning] Could not find Read or Index number in the cloud file :( Please specify Read num or Index num."
                            )
                            while True:
                                input_data = input("Specify ID in R1, R2, I1, (I2)")
                                if (
                                    (input_data == "R1")
                                    or (input_data == "R2")
                                    or (input_data == "I1")
                                    or (input_data == "I2")
                                ):
                                    result.read = input_data
                                    break
            else:
                print("[ERROR] Unknown Read or Index number.")
                quit()
        else:
            for result in results:
                result.read = "Unknown"
        return results
