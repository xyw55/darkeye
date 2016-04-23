#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-03-28 00:42:29
# @Author  : Yiwei Xia (you@example.org)
# @Link    : http://example.org
# @Version : $Id$

import os

infilename = "pcap3.pcap"
infid = open(infilename, "r")
outfilename = "rpcap3.pcap"
outfid = open(outfilename, "w")

for line in infid.readlines():
    if line != "\r\n":
        print line
        outfid.write(line.replace("\n","").replace("\r", ""))
    else:
        outfid.write(line)

infid.close()
outfid.close()

