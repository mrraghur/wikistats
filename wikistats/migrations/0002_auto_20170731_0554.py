# -*- coding: utf-8 -*-
# Generated by Django 1.9 on 2017-07-31 05:54
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('wikistats', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ArticleStats',
            fields=[
                ('article', models.TextField(primary_key=True, serialize=False)),
                ('viewCount', models.IntegerField(default=0)),
            ],
        ),
        migrations.DeleteModel(
            name='ArctileStats',
        ),
        migrations.RemoveField(
            model_name='articleperdaystats',
            name='id',
        ),
        migrations.AlterField(
            model_name='articleperdaystats',
            name='article',
            field=models.TextField(primary_key=True, serialize=False),
        ),
    ]
