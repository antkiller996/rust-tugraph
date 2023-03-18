FROM tugraph/tugraph-compile-centos7:latest

# use to set swapon
RUN yum -y install sudo

# install libclang-*.so
RUN yum -y remove git && yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm && yum -y install git \
    && yum -y install centos-release-scl \
    && yum -y install llvm-toolset-7 \
    && yum -y install llvm-toolset-7.0-clang-libs-7.0.1-1.el7.x86_64 \
    && rm -rf /var/cache/yum/*

# export llvm-toolset-7 environment variables
ENV PATH="/opt/rh/llvm-toolset-7/root/usr/bin:/opt/rh/llvm-toolset-7/root/usr/sbin${PATH:+:${PATH}}"
ENV LD_LIBRARY_PATH="/opt/rh/llvm-toolset-7/root/usr/lib64${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
ENV MANPATH="/opt/rh/llvm-toolset-7/root/usr/share/man:${MANPATH:-}"
ENV PKG_CONFIG_PATH="/opt/rh/llvm-toolset-7/root/usr/lib64/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
ENV PYTHONPATH="/opt/rh/llvm-toolset-7/root/usr/lib/python2.7/site-packages${PYTHONPATH:+:${PYTHONPATH}}"