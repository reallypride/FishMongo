FishMongo是MongoDB的数据模型,使用递增id作为主键，兼容Django数据模型的大部分语法。

1、快速开始

*基本操作*
>     class Category(Document):
>         name = Field()

>     class Article(Document):
>         title = Field()
>         content = Field()
>         tags = ArrayField()
>         category = ForeignKey(Category)
> 	      created = DatetimeField()
>         viewnum = IntegerField()

>     category = Category(name="Fruit")
>     category.save()
>     category.id

输出：1

>     article = Article()
>     article.title = "Apple"
>     article.tags = "food apple fruit"
>     article.content = "Apple Apple"
>     article.category = category
>     article.save()
>     article.viewnum

输出：0

>     article.incnum(viewnum=2)
>     article.viewnum

输出：2

>     article.set_value(title="Pear")
>     article.title

输出：Pear

>     article.tags

输出：["food", "apple", "fruit"]

*查询*

>     obj = Article.objects.get(id=1)
>     cursor = Article.objects.all().order_by('-id')
>     cursor = Article.objects.filter(created__lt=datetime.datetime.now())
>     cursor = Article.objects.filter(id__in=[1,2,3])

*删除*

>     obj.delete() #该方法和Django一样，会进行关联删除

*通用类型*

>     class Complex(Document):
>         content_object = GenerForeignKey()

>     c1 = Complex(content_object=category)
>     c1.save()

>     c2 = Complex(content_object=article)
>     c2.save()

>     cursor = Complex.objects.filter(content_object=category)

*信号*

>     @receiver(post_save, sender=Article)
>     def Article_post_save(sender, **kwargs):
>         obj = kwargs['instance']
>         created = kwargs['created']
>     	  if created:
>             #other operation