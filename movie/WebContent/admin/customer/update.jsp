<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>添加管理员</title>
<link rel="stylesheet" type="text/css"
	href="${pageContext.request.contextPath}/static/plugins/bootstrap/css/bootstrap.css" />
<script type="text/javascript">
</script>
</head>
<body>
	<div class="container">
		<div class="row">
			<div class="col-xs-12">
				<div class="view">
					<h3>修改客户信息</h3>
				</div>
				<form role="form" action="${pageContext.request.contextPath}/admin/customerServlet?method=update" method="post" class="form-horizontal">
				<table class="table table-striped">
					<tr>
						<td>username：</td>
						<td>
							<div class="form-group">
							<input class="form-control" name="username" type="text"
							placeholder="username" value="${bean.username }" />
							</div>
						</td>
						<td></td>
					</tr>
					<tr>
						<td>password：</td>
						<td>
							<div class="form-group">
							<input class="form-control" name="password" type="text"
							placeholder="password" value="${bean.password }" />
							</div>
						</td>
						<td></td>
					</tr>
					<tr>
						<td>name：</td>
						<td>
							<div class="form-group">
							<input name="name"
							class="form-control" value="${bean.name }" placeholder="name">
							</div>
						</td>
						<td></td>
					</tr>
					<tr>
						<td>email：</td>
						<td>
							<div class="form-group">
							<input name="email"
							class="form-control" value="${bean.email }" placeholder="email">
							</div>
						</td>
						<td></td>
					</tr>
					<tr>
						<td></td>
						<td>
							<input type="submit"
								class="btn btn-primary btn-sm" value="提交"/>
						</td>
						<td></td>
					</tr>
				</table>
				<c:if test="${bean!=null }">
					<input type="hidden" name="productTypeBean.id" value="${bean.id }"/>
				</c:if>
				</form>
			</div>
		</div>
	</div>

	<script
		src="${pageContext.request.contextPath}/static/js/jquery-1.11.1.js"
		type="text/javascript" charset="utf-8"></script>
	<script
		src="${pageContext.request.contextPath}/static/plugins/bootstrap/js/bootstrap.js"
		type="text/javascript" charset="utf-8"></script>
	<script
		src="${pageContext.request.contextPath}/static/plugins/jquery-validation/dist/jquery.validate.js"
		type="text/javascript" charset="utf-8"></script>

	<script type="text/javascript">
	
	$(function() {
		var MyValidator = function() {
			var handleSubmit = function() {
				$('.form-horizontal').validate(
						{
							errorElement : 'span',
							errorClass : 'help-block',
							focusInvalid : false,
							rules : {
								'username' : {
									required : true
								},
								'password' : {
									required : true
								},
								'name' : {
									required : true
								},
								'email' : {
									required : true,
									email : true
								}
							},
							messages : {
								'username' : {
									required : "请输入用户名"
								},
								'password' : {
									required : "请输入密码"
								},
								'name' : {
									required : "请输入姓名"
								},
								'email' : {
									required : "请输入邮箱",
									email : "邮箱格式不正确"
								}
							},

							highlight : function(element) {
								$(element).closest('.form-group').addClass(
										'has-error');
							},

							success : function(label) {
								label.closest('.form-group').removeClass(
										'has-error');
								label.remove();
							},

							errorPlacement : function(error, element) {
								element.parent('div').append(error);
							},

							submitHandler : function(form) {
								form.submit();
							}
						});

				$('.form-horizontal input').keypress(function(e) {
					if (e.which == 13) {
						if ($('.form-horizontal').validate().form()) {
							$('.form-horizontal').submit();
						}
						return false;
					}
				});
			};
			return {
				init : function() {
					handleSubmit();
				}
			};

		}();

		MyValidator.init();
	});
	</script>
</body>
</html>