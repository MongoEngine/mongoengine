# sitelib for noarch packages, sitearch for others (remove the unneeded one)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}

%define srcname mongoengine

Name:           python-%{srcname}
Version:        0.5.3
Release:        1%{?dist}
Summary:        A Python Document-Object Mapper for working with MongoDB

Group:          Development/Libraries
License:        MIT
URL:            https://github.com/namlook/mongoengine
Source0:        %{srcname}-%{version}.tar.bz2

BuildRequires:  python-devel
BuildRequires:  python-setuptools

Requires:       mongodb
Requires:       pymongo
Requires:       python-blinker
Requires:       python-imaging

%description
MongoEngine is an ORM-like layer on top of PyMongo.

%prep
%setup -q -n %{srcname}-%{version}


%build
# Remove CFLAGS=... for noarch packages (unneeded)
CFLAGS="$RPM_OPT_FLAGS" %{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc docs AUTHORS LICENSE README.rst
# For noarch packages: sitelib
 %{python_sitelib}/*
# For arch-specific packages: sitearch
# %{python_sitearch}/*

%changelog
* Thu Oct 27 2011 Pau Aliagas <linuxnow@gmail.com> 0.5.3-1
- Update to latest dev version
* Add PIL dependency for ImageField

* Wed Oct 12 2011 Pau Aliagas <linuxnow@gmail.com> 0.5.2-1
- Update version

* Fri Sep 23 2011 Pau Aliagas <linuxnow@gmail.com> 0.5.0-1
- Initial version
