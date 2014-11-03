function CL = cl_viz(I,M)

[h w] = size(M);
CL = zeros(h,w);

for x = 1:w
	for y = 1:h
		%NB = getNB(x,y,h,w,M);
		
		if(x > 1 && M(y,x-1) ~= M(y,x))
			continue
		elseif (y > 1 && M(y-1,x) ~= M(y,x))
			continue
		elseif (x < w && M(y,x+1) ~= M(y,x))
			continue
		elseif (y < h && M(y+1,x) ~= M(y,x))
			continue
		endif
		
		CL(y,x) = I(y,x);
	end	
end
	
end

function NB = getNB(x,y,h,w,M)
NB = M(max(1,y-1):min(y+1,h),max(1,x-1):min(w,x+1));
end

function tf = isBorder(NB)
NB = NB .- NB(1);
tf = sum(sum(NB)) ~= 0;
end