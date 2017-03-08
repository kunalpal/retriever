# retriever

"""Retriever script for direct download of Bioclim data"""
from builtins import range

from retriever.lib.templates import Script
from retriever.lib.models import Table, Cleanup


class main(Script):
    def __init__(self, **kwargs):
        Script.__init__(self, **kwargs)
        self.name = "Bioclim 2.5 Minute Climate Data"
        self.shortname = "bioclim"
        self.ref = "http://worldclim.org/bioclim"
        self.urls = {"climate": "http://biogeo.ucdavis.edu/data/climate/worldclim/1_4/grid/cur/bio_2-5m_bil.zip"}
        self.description = "Bioclimatic variables that are derived from the monthly temperature and rainfall values in order to generate more biologically meaningful variables."
        self.citation = "Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and A. Jarvis, 2005. Very high resolution interpolated climate surfaces for global land areas. International Journal of Climatology 25: 1965-1978."
        self.retriever_minimum_version = '2.0.dev'
        self.version = '1.1.0'
        self.tags = ["climate"]
        self.spatial = "raster"

    def download(self, engine=None, debug=False, use_cache=True):
        if engine.name not in ["Download Only", "PostGIS"]:
            raise Exception("The Bioclim dataset contains only non-tabular data files.\n "
                            "It can only be used with the 'download only' or 'postgis' engine.")
        _engine = engine
        Script.download(self, engine, debug, use_cache)
        file_names = []
        for file_num in range(1, 20):
            for ext in (['bil', 'hdr']):
                file_names += ["bio{0}.{1}".format(file_num, ext)]
        _engine.download_files_from_archive(self.urls["climate"], file_names)
        if _engine.name is "Download Only":
            _engine.register_files(file_names)
        elif _engine.name is "PostGIS":
            hdr_files = [file_name[:-4] for file_name in file_names
                         if file_name[-3:] == "hdr"]
            self.tables = {}
            for hdr_file in hdr_files:
                with open(_engine.format_filename(hdr_file + ".hdr"), "r") as hdr_content:
                    table = {}
                    for line in hdr_content:
                        if "Variable" in line:
                            table["name"] = line.split("Variable")[-1].split("=")[0].strip()
                    hdr_content.close()
                    if "name" in table.keys():
                        _engine.insert_data_from_file(_engine.format_filename(hdr_file + ".bil"),
                                                      table_name=table["name"])


SCRIPT = main()

