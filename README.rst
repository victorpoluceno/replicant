Replicant
=============

Couchdb to sql live replicanting tool

Install::

    sh setup.sh

Edit /etc/couchdb/local.ini and change bind_address to 0.0.0.0 then::

    virtualenv env
    . env/bin/activate
    pip install -r /vagrant/requirements.txt

Create the initial database::

    create table test1 (id TEXTO PRIMARY KEY, field_a INTEGER, field_b TEXT);