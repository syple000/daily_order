from .conf import Conf
from .reqs import TBFactoryReqs

if __name__ == '__main__':
    conf = Conf()
    tb_fac_reqs = TBFactoryReqs(conf)
    # tb_fac_reqs.createOrderExportTask()
    download_url = tb_fac_reqs.taskRecord(64509589)
    tb_fac_reqs.download(download_url, 'test')